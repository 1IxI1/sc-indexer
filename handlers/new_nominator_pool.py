"""
Handler for nominator pool contract.
With accounting.
https://github.com/ton-blockchain/nominator-pool/
"""

import base64
import math
from pprint import pprint

from loguru import logger
from pytoniq.liteclient import BlockIdExt, LiteClient
from pytoniq_core.boc import Address, Cell, Slice
from pytoniq_core.boc.hashmap import HashMap
from sqlalchemy import and_, delete, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from contracts_db.database import Account, Booking, Nominator, NominatorPool, SubAccount
from core.utils import addr_hash_wc0_parse, empty_parse, nanostr
from handlers.handler_types import DBSession
from mainnet_db.database import (
    Block,
    Message,
    MessageContent,
    Transaction,
    TransactionMessage,
)


def nominator_value_parse(src: Slice) -> tuple[int, int]:
    # nominator#_ deposit:Coins pending_deposit:Coins = Nominator;
    deposit = src.load_coins() or 0
    pending_deposit = src.load_coins() or 0
    return deposit, pending_deposit


def parse_pool(data: Cell):
    # pool_data#_ state:uint8 nominators_count:uint16
    #             stake_amount_sent:Coins validator_amount:Coins
    #             config:^PoolConfig nominators:Nominators
    #             withdraw_requests:WithdrawalRequests
    #             stake_at:uint32 saved_validator_set_hash:uint256
    #             validator_set_changes_count:uint8 validator_set_change_time:uint32
    #             stake_held_for:uint32 config_proposal_votings:Votings
    #         = Storage;

    ds = data.begin_parse()
    state = ds.load_uint(8)
    nominators_count = ds.load_uint(16)
    stake_amount_sent = ds.load_coins()
    validator_amount = ds.load_coins()
    config = ds.load_ref()

    # nominator#_ deposit:Coins pending_deposit:Coins = Nominator;
    # nominators#_ _:(HashmapE 256 Nominator) = Nominators;  // 256 is the addr hash

    nominators_cell = ds.load_maybe_ref()
    withdraw_requests_cell = ds.load_maybe_ref()

    return (
        state,
        nominators_count,
        stake_amount_sent,
        validator_amount,
        config,
        nominators_cell,
        withdraw_requests_cell,
    )


async def handler(
    origin_db: DBSession,
    result_db: DBSession,
    pool_address_str: str,
    balance: int,
    data_hash: str,
    lite_client: LiteClient,
    utime: int,
):
    async with origin_db() as origin_conn, result_db() as result_conn:
        await handle_nominator_pool(
            origin_conn,
            result_conn,
            pool_address_str,
            balance,
            data_hash,
            lite_client,
            utime,
        )


async def handle_nominator_pool(
    origin_conn: AsyncSession,
    result_conn: AsyncSession,
    pool_address_str: str,
    balance: int,
    data_hash: str,
    lite_client: LiteClient,
    processing_from_time: int,
):

    # use it to clean data if pool cannot be processed
    async def delete_pool_with_nominators():
        query = delete(Nominator).filter_by(pool_address=pool_address_str)
        await result_conn.execute(query)
        query = delete(NominatorPool).filter_by(account=pool_address_str)
        await result_conn.execute(query)

    pool_address = Address(pool_address_str)

    try:
        account = await lite_client.get_account_state(pool_address)
        if not account.state.state_init or not account.state.state_init.data:
            raise Exception("No data in state init")
    except Exception:
        logger.info("No account state init was found for pool " + pool_address_str)
        await delete_pool_with_nominators()
        return

    data = account.state.state_init.data

    (
        state,
        nominators_count,
        stake_amount_sent,
        validator_amount,
        config,
        nominators_cell,
        withdraw_requests_cell,
    ) = parse_pool(data)

    # first, update the pool data because it may be empty later
    query = (
        select(Account, NominatorPool)
        .select_from(Account)
        .join(NominatorPool, NominatorPool.account_id == Account.account_id)
        .filter(Account.account == pool_address_str)
    )
    res = await result_conn.execute(query)
    account, pool = res.all()[0]

    if not account:
        account = Account(
            account=pool_address_str,
            account_type="nominator_pool",
            balance=balance,
        )
        result_conn.add(account)
        await result_conn.flush()
        pool = NominatorPool(account_id=account.account_id)

    account.balance = balance
    pool.validator_amount = validator_amount
    pool.stake_amount_sent = stake_amount_sent
    pool.nominators_count = nominators_count

    result_conn.add(pool)
    await result_conn.commit()

    # then additional value checks
    if not nominators_cell:
        logger.info("No nominators_cell in pool " + pool_address_str)
        # await delete_pool_with_nominators()
        return

    nominators_dict = HashMap.parse(
        dict_cell=nominators_cell.begin_parse(),
        key_length=256,
        key_deserializer=addr_hash_wc0_parse,
        value_deserializer=nominator_value_parse,
    )
    if not nominators_dict:
        logger.info("Invalid nominators_dict in pool " + pool_address_str)
        # await delete_pool_with_nominators()
        return

    if withdraw_requests_cell:
        # using as list, with no meaningful value
        # requested_withdrawals#_ _:(HashmapE 256 Cell) = WithdrawalRequests; // addr -> none
        withdraw_requests_dict = HashMap.parse(
            dict_cell=withdraw_requests_cell.begin_parse(),
            key_length=256,
            key_deserializer=empty_parse,
        )

        if not withdraw_requests_dict:
            logger.info("Invalid withdraw_requests in pool " + pool_address_str)
            # await delete_pool_with_nominators()
            return

    # first, we create pool account and subaccounts for every active nominator
    # (because their balance and pending_balance exist)

    # then, we get all the transactions came from/to our pool
    # and fill the deposit/withdraw bookings
    # and by the way we create inactive subaccounts to ref them in bookings

    # then we go for every income (recover_stake) tx from elector to pool
    # and run get method on pool just a block before income
    # to get the pool's data and nominators with their shares
    # with those shares, we add all nominator_income bookings into accounting

    # probably, it needs some sorting. so we first compile data in dicts
    # and at the end sort and write into db

    nominators = {}
    for nominator, (balance, pending_balance) in nominators_dict.items():
        subaccount_typed = Nominator(
            balance=balance,
            pending_balance=pending_balance,
        )
        nominators[nominator] = subaccount_typed

    q = (
        select(
            Message,
            MessageContent.body,
            Transaction.description,
            Transaction.block_seqno,
        )
        .select_from(Message)
        .join(MessageContent, Message.body_hash == MessageContent.hash)
        .join(TransactionMessage, Message.hash == TransactionMessage.message_hash)
        .join(Transaction, TransactionMessage.transaction_hash == Transaction.hash)
    )
    q = q.filter(Message.created_at > processing_from_time)
    q = q.order_by(Message.created_lt)

    query_msgs_to_pool = q.filter(Message.destination == pool_address_str)
    query_msgs_from_pool = q.filter(Message.source == pool_address_str)

    res_to_pool = await origin_conn.execute(query_msgs_to_pool)
    res_from_pool = await origin_conn.execute(query_msgs_from_pool)
    msgs_to_pool = res_to_pool.all()
    msgs_from_pool = res_from_pool.all()

    bookings = {}
    withdrawal_requests = {}

    for msg, body, descr, block_seqno in msgs_to_pool:
        logger.debug(f"     new tx (to) with lt {msg.created_lt} at {msg.created_at}")
        try:
            exit_code = (  # bitwise or to catch any other than 0
                descr["compute_ph"]["exit_code"] | descr["action"]["result_code"]
            )
        except:
            logger.debug(f"Failed to parse tx description: {descr}")
            continue

        if exit_code != 0:
            logger.debug(f"Failed tx: {exit_code} at {msg.created_lt}")
            continue

        body_boc = Cell.from_boc(body)[0].begin_parse()
        op = body_boc.load_uint(32)
        if op == 0:
            first_letter = chr(body_boc.load_uint(8))[0]
            if first_letter == "d":
                bookings[msg.created_lt] = {
                    "utime": msg.created_at,
                    "subaccount_address": msg.source,
                    "debit": msg.value - 10**9,
                    "credit": 0,
                    "type": "nominator_deposit",
                }
                logger.info(
                    "Deposit with value %s at %s for %s on %s"
                    % (nanostr(msg.value), msg.created_at, msg.source, pool_address_str)
                )

            elif first_letter == "w":
                if msg.source not in withdrawal_requests:
                    withdrawal_requests[msg.source] = []
                withdrawal_requests[msg.source].append(msg.created_at)
                logger.info(
                    "Withdraw req at %s from %s on %s"
                    % (msg.created_at, msg.source, pool_address_str)
                )
        elif op == 0xF96F7324:  # recover_stake_ok (i.e. income)
            assert (  # came from elector
                msg.source
                == "-1:3333333333333333333333333333333333333333333333333333333333333333"
            )

            wc = -1
            shard = -9223372036854775808
            prev_block_seqno = block_seqno - 1

            q = select(Block).filter(
                Block.seqno == prev_block_seqno, Block.workchain == wc
            )
            res = await origin_conn.execute(q)
            block = res.scalar()
            if not block:
                logger.info("No block at %s" % prev_block_seqno)
                continue

            root_hash = base64.b64decode(block.root_hash)
            file_hash = base64.b64decode(block.file_hash)

            on_block = BlockIdExt(wc, shard, prev_block_seqno, root_hash, file_hash)

            res = await lite_client.run_get_method(
                pool_address_str, "get_pool_data", [], on_block
            )
            stake_amount_sent_before = res[2]
            validator_share = res[5]
            nominators_before = res[9]

            _nominators_dict = HashMap.parse(
                dict_cell=nominators_before.begin_parse(),
                key_length=256,
                key_deserializer=addr_hash_wc0_parse,
                value_deserializer=nominator_value_parse,
            )
            if not _nominators_dict:
                logger.info("No nominators at %s" % on_block)
                continue

            reward = msg.value - stake_amount_sent_before
            validator_reward = int(reward * validator_share / 10000)
            nominators_reward = reward - validator_reward

            for nominator, (balance, pending_balance) in _nominators_dict.items():
                share = balance / stake_amount_sent_before
                his_reward = int(share * nominators_reward)
                bookings[msg.created_lt] = {
                    "utime": msg.created_at,
                    "subaccount_address": nominator.to_str(False).upper(),
                    "debit": his_reward,
                    "credit": 0,
                    "type": "nominator_income",
                }

    for msg, body, descr, block_seqno in msgs_from_pool:
        logger.debug(
            f"     new tx (from pool) with lt {msg.created_lt} at {msg.created_at}"
        )
        # pool sends withdrawals in bounceable mode
        if descr["bounce"] is not True:
            logger.debug(f"Non-bounceable, skip")
            continue
        # and without body (empty cell). excesses will have op
        if body != "te6cckEBAQEAAgAAAEysuc0=":
            logger.debug(f"Non-empty body, skip")
            continue
        # and it's more than 1 TON
        if msg.value < 10**9:
            logger.debug(f"Less than 1 TON, skip")
            continue

        if not msg.destination in withdrawal_requests:
            logger.debug(f"No requests from {msg.destination}, skip")
            continue

        # search for withdrawal request max 36 hours ago (2 rounds)
        min_at = msg.created_at - 36 * 3600
        max_at = msg.created_at
        for req_at in withdrawal_requests[msg.destination]:
            if req_at > min_at and req_at <= max_at:
                logger.debug(f"Found request from {msg.destination} at {req_at}")
                break
        else:
            logger.debug(f"No requests from {msg.destination} in last 36 hours, skip")
            continue

        logger.debug(
            "Found withdrawal msg from pool %s, amount %s, receiver %s at %s"
            % (msg.source, msg.value, msg.destination, msg.created_at)
        )

        bookings[msg.created_lt] = {
            "utime": msg.created_at,
            "subaccount_address": msg.source,
            "debit": 0,
            "credit": msg.value,
            "type": "nominator_withdrawal",
        }
    # sort bookings by lt
    bookings = dict(sorted(bookings.items()))
    pprint(bookings)


nominator_pool_handler = (
    "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=",
    handler,
)
