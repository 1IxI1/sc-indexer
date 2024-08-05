"""
Handler for dedust liquidity pool smart contract.
(no source code available)
"""

import asyncio
import base64
import json
import math
from hashlib import sha256
from pprint import pprint
from typing import NamedTuple

import aiometer
from loguru import logger
from pytoniq.liteclient import BlockIdExt, LiteClient
from pytoniq_core.boc import Address, Cell, Slice
from pytoniq_core.boc.hashmap import HashMap
from sqlalchemy import and_, delete, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from contracts_db.database import Account, LPool, LPoolAsset, LPoolWithAssets, SubAccount
from core.settings import settings
from core.utils import addr_hash_wc0_parse, empty_parse, nanostr
from handlers.handler_types import DBSession, HandlerArgs
from mainnet_db.database import JettonMaster


async def handler(args: HandlerArgs):
    async with args.origin_db() as origin_conn, args.result_db() as result_conn:
        try:
            await handle_dedust_pool(
                origin_conn,
                result_conn,
                args.address,
                args.balance,
                args.data_hash,
                args.lite_client,
                args.utime,
            )
        except Exception as e:
            logger.error(
                f"Unknown error while handling liquidity pool {args.address} from {args.utime}: {e}"
            )


async def handle_dedust_pool(
    origin_conn: AsyncSession,
    result_conn: AsyncSession,
    lpool_addr_str: str,
    balance: int,
    data_hash: str,
    lite_client: LiteClient,
    processing_from_time: int,
):
    account = await result_conn.execute(
        select(LPoolWithAssets).filter(LPoolWithAssets.account == lpool_addr_str)
    )
    existing = account.scalars().first()
    if existing:
        logger.debug(f"LPool {lpool_addr_str} already exists")
        return

    # use it to clean data if lpool cannot be processed
    async def delete_lpool_with_tokens():
        account_to_delete = await result_conn.execute(
            select(LPoolWithAssets).filter(LPoolWithAssets.account == lpool_addr_str)
        )
        account_to_delete = account_to_delete.scalars().first()
        if account_to_delete:
            await result_conn.delete(account_to_delete)
            await result_conn.commit()
            logger.warning(f"Account {lpool_addr_str} was deleted")
        else:
            logger.warning(f"Account {lpool_addr_str} not found in db to delete")

    try:
        res1 = await lite_client.run_get_method(lpool_addr_str, "get_assets", [])
        res2 = await lite_client.run_get_method(lpool_addr_str, "get_reserves", [])
    except Exception as e:
        logger.error(f"Failed to run get_assets on {lpool_addr_str}: {e}")
        return

    async def parse_asset(asset: Slice, reverve:int):
        # native$0000 = Asset;
        # jetton$0001 workchain_id:int8 address:uint256 = Asset;
        asset_type = asset.load_uint(4)
        if asset_type == 0:
            return {
                "type": "ton",
                "reserve": reverve,
                "addr": "",
                "symbol": "TON",
            }
        if asset_type == 1:
            workchain_id = asset.load_int(8)
            address = asset.load_bytes(32)
            addr = f"{workchain_id}:{address.hex().upper()}"
            symbol = ""
            try:
                # get symbol from origin db, from `jetton_masters`
                content_res = await origin_conn.execute(
                    select(JettonMaster.jetton_content).filter(
                        JettonMaster.address == addr
                    )
                )
                # it's jsonb
                content_res = content_res.scalars().first()
                if content_res:
                    symbol = content_res.get("symbol", "")
            except Exception as e:
                logger.error(f"Failed to get symbol for {addr}: {e}")
            return {
                "type": "jetton",
                "reserve": reverve,
                "addr": "",
                "addr": addr,
                "symbol": symbol,
            }
        else:
            raise Exception(f"Unknown dedust asset type: {asset_type}")

    asset0 = await parse_asset(res1[0], res2[0])
    asset1 = await parse_asset(res1[1], res2[1])

    # insert into db pool and assets

    lpool_account = LPoolWithAssets(
        account=lpool_addr_str,
        lpool_type="dedust",
        asset1_address=asset0["addr"],
        asset1_symbol=asset0["symbol"],
        asset1_reserve=asset0["reserve"],
        asset2_address=asset1["addr"],
        asset2_symbol=asset1["symbol"],
        asset2_reserve=asset1["reserve"],
        balance=balance,
    )

    result_conn.add(lpool_account)
    await result_conn.commit()
    logger.info(f"Lpool (dedust) {lpool_addr_str} was added")

    return 

    lpool_account = Account(
        account=lpool_addr_str,
        account_type="l_pool",
        balance=balance,
    )
    result_conn.add(lpool_account)
    await result_conn.flush()
    lpool = LPool(account_id=lpool_account.account_id, pool_type="dedust")

    asset0_subaccount = SubAccount(
        owner=asset0["addr"],
        subaccount_type="l_pool_asset",
        parent_account_id=lpool_account.account_id,
    )
    asset1_subaccount = SubAccount(
        owner=asset1["addr"],
        subaccount_type="l_pool_asset",
        parent_account_id=lpool_account.account_id,
    )
    result_conn.add_all([lpool, asset0_subaccount, asset1_subaccount])
    await result_conn.flush()

    lpool_assets = [
        LPoolAsset(
            subaccount_id=asset0_subaccount.subaccount_id,
            jetton_symbol=asset0["symbol"],
        ),
        LPoolAsset(
            subaccount_id=asset1_subaccount.subaccount_id,
            jetton_symbol=asset1["symbol"],
        ),
    ]

    result_conn.add_all(lpool_assets)
    await result_conn.commit()
    logger.info(f"Lpool (dedust) {lpool_addr_str} was added")

dedust_pool_handler = (
    "EnUJW22jkRKSQG9PQ4b554AJm4VMbe6e4old3OcJJ8E=",
    handler,
)
