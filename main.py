import asyncio
import functools
import json
import os
import sys
from time import time
from progress.bar import IncrementalBar

import aiometer
from dotenv import load_dotenv
from loguru import logger
from pytoniq.liteclient import LiteClient
from sqlalchemy import select, text
from sqlalchemy.schema import CreateSchema

from contracts_db.database import Base as ContractsBase
from core.connections import SessionMaker_Origin, SessionMaker_Result, engine_result
from core.localdb import localdb
from core.processors import CallHandlerArgs, call_handler
from core.settings import settings
from handlers import handlers as contract_handlers
from handlers.handler_types import HandlerArgs
from mainnet_db.database import LatestAccountState
from pytoniq_core.boc.hashmap import HashMap
from pytoniq_core.boc import Address, Cell, Slice
from core.utils import addr_hash_wc0_parse
from handlers.new_nominator_pool import parse_pool


load_dotenv()


# use LS to get contracts' data
with open(settings.config_path) as config_file:
    config = json.loads(config_file.read())
lite_client = LiteClient.from_config(config, timeout=30)  # i=2 for mainnet config

print(contract_handlers.keys())

PERIOD = 30
CHUNK_SIZE = 5


@logger.catch()
async def run():
    localdb.read()

    contract_types = contract_handlers.keys()
    logger.warning(
        f"Start handling {len(contract_types)} contract types from second {localdb.index_second}."
    )

    async with SessionMaker_Origin() as origin_db:
        query = (
            select(
                LatestAccountState.account,
                LatestAccountState.balance,
                LatestAccountState.code_hash,
                LatestAccountState.data_hash,
                LatestAccountState.timestamp,
            )
            .filter(LatestAccountState.code_hash.in_(contract_types))
            .filter(LatestAccountState.timestamp > localdb.index_second)
            .order_by(LatestAccountState.timestamp)
            # .filter(  # DEBUG
            #     LatestAccountState.account
            #     == "-1:C3927B436211AF4556703A6928BA9232B78CC8AA2EF89563A42542411DB4830E"
            # )
        )

        res = await origin_db.execute(query)
        all_accounts = res.all()
        logger.warning(f"Found {len(all_accounts)} accounts of described types.")

    # tasks = []  # gathering buffer
    processing_timestamp = 0  # in case of error - save chunk's timestamp
    argss = []
    for i, (account, balance, code_hash, data_hash, timestamp) in enumerate(
        all_accounts
    ):
        # tasks.append(
        #     call_handler(
        #         code_hash,
        #         SessionMaker_Origin,
        #         SessionMaker_Result,
        #         account,
        #         balance,
        #         data_hash,
        #         lite_client,
        #         localdb.index_second,
        #     )
        # )
        argss.append(
            CallHandlerArgs(
                handler_args=HandlerArgs(
                    origin_db=SessionMaker_Origin,
                    result_db=SessionMaker_Result,
                    address=account,
                    balance=balance,
                    data_hash=data_hash,
                    lite_client=lite_client,
                    utime=localdb.index_second,
                ),
                code_hash=code_hash,
            )
        )

        # if not processing_timestamp:
        #     processing_timestamp = timestamp - 1

        # if len(tasks) >= CHUNK_SIZE or i == len(all_accounts) - 1:
        #     try:
        #         pass
        #     except Exception as e:
        #         logger.error(
        #             "Something went wrong on indexing around timestamp",
        #             processing_timestamp,
        #         )
        #         logger.error(f"Saving second {processing_timestamp} to local db")
        #         localdb.index_second = processing_timestamp
        #         localdb.write()
        #         processing_timestamp = 0
        #         raise e
        #     await asyncio.gather(*tasks)
        #     tasks = []
        #     processing_timestamp = 0

    # await aiometer.run_on_each(call_handler, argss, max_at_once=CHUNK_SIZE)

    
    # the bar will be in stdout only
    bar = IncrementalBar(f'Indexing from {localdb.index_second}', max=len(argss))

    done = 0
    started_at = time()
    async with aiometer.amap(
        call_handler,
        argss,
        max_at_once=9,
        max_per_second=3,
    ) as results:
        async for _ in results:
            done += 1
            elapsed = time() - started_at
            speed = done / elapsed
            bar.next()
            logger.info(f"Processed {done}/{len(all_accounts)}, {speed:.2f}/sec")

    bar.finish()

    if all_accounts:
        last_timestamp = all_accounts[-1][4]
        localdb.index_second = last_timestamp
        localdb.write()
        logger.warning(
            f"Finished index cycle and saved second {localdb.index_second} to local db"
        )
    else:
        logger.warning(
            f"Finished index cycle. Nothing since the second {localdb.index_second}."
        )


async def connect_db():
    async with engine_result.begin() as conn:
        await conn.execute(CreateSchema("account_types", if_not_exists=True))
        await conn.execute(CreateSchema("subaccount_types", if_not_exists=True))
        await conn.run_sync(ContractsBase.metadata.create_all)


async def main():
    global lite_client
    logger.critical(
        "Starting Smart Contracts Indexer from %s db into %s db"
        % (settings.db_origin_name, settings.db_result_name)
    )

    await connect_db()
    await lite_client.connect()

    # res = await lite_client.run_get_method(
    #     "-1:C3927B436211AF4556703A6928BA9232B78CC8AA2EF89563A42542411DB4830E",
    #     "get_pool_data",
    #     [],
    # )
    res = await lite_client.get_account_state("-1:20429A7BA4A0FD1B306B72BDE3BBD7DA3E31161CF7AB859E2D1694A20E80D420")
    if res.state.state_init and res.state.state_init.data:
        (
            state,
            nominators_count,
            stake_amount_sent,
            validator_amount,
            config,
            nominators_cell,
            withdraw_requests_cell,
        ) = parse_pool(res.state.state_init.data)

        def nominator_value_parse(src: Slice) -> tuple[int, int]:
            # nominator#_ deposit:Coins pending_deposit:Coins = Nominator;
            deposit = src.load_coins() or 0
            pending_deposit = src.load_coins() or 0
            return deposit, pending_deposit
        
        if nominators_cell:
            nominators_dict = HashMap.parse(
                dict_cell=nominators_cell.begin_parse(),
                key_length=256,
                key_deserializer=addr_hash_wc0_parse,
                value_deserializer=nominator_value_parse,
            )
            print(nominators_dict)


    while True:
        await run()
        await asyncio.sleep(PERIOD)


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)

    logger.remove()

    logger.add(
        "logs/sci_{time:YYYY_MM_DD}.log",
        level="DEBUG",
        # format="{time} {level} {message}",
        format="{time:YYYY-MM-DD at HH:mm:ss} | {file}:{line} | {level} | {message}",
        backtrace=True,
        diagnose=True,
        rotation="01:00",
        compression="gz"
    )
    asyncio.run(main())
