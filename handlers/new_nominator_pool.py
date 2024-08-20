"""
Handler for nominator pool contract.
With accounting.
https://github.com/ton-blockchain/nominator-pool/
"""

from typing import NamedTuple
import asyncio
import aiometer
import base64
import json
import math
from hashlib import sha256
from pprint import pprint

from loguru import logger
from pytoniq.liteclient import BlockIdExt, LiteClient
from pytoniq_core.boc import Address, Cell, Slice
from pytoniq_core.boc.hashmap import HashMap
from sqlalchemy import and_, delete, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from contracts_db.database import Account, Booking, Nominator, NominatorPool, SubAccount
from core.settings import settings
from core.utils import addr_hash_wc0_parse, empty_parse, nanostr
from handlers.handler_types import DBSession, HandlerArgs
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


async def handler(args: HandlerArgs):
    async with args.origin_db() as origin_conn, args.result_db() as result_conn:
        try:
            await handle_nominator_pool(
                origin_conn,
                result_conn,
                args.address,
                args.balance,
                args.data_hash,
                args.lite_client,
                args.utime,
            )
        except Exception as e:
            logger.error(f"Unknown error while handling pool {args.address} from {args.utime}: {e}")

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
        account_to_delete = await result_conn.execute(select(Account).filter(Account.account == pool_address_str))
        account_to_delete = account_to_delete.scalars().first()
        if account_to_delete:
            await result_conn.delete(account_to_delete)
            await result_conn.commit()
            logger.warning(f"Account {pool_address_str} was deleted")
        else:
            logger.warning(f"Account {pool_address_str} not found in db to delete")
        # query = delete(Nominator).filter_by(pool_address=pool_address_str)
        # await result_conn.execute(query)
        # query = delete(NominatorPool).filter_by(account=pool_address_str)
        # await result_conn.execute(query)

    pool_address = Address(pool_address_str)

    try:
        pool_account = await lite_client.get_account_state(pool_address)
        if not pool_account.state.state_init or not pool_account.state.state_init.data:
            raise Exception("No data in state init")
    except Exception as e:
        logger.warning(f"Error while getting account state for {pool_address_str}: {e}. Reconnecting lite client")
        with open(settings.config_path) as config_file:
            config = json.loads(config_file.read())
        lite_client = LiteClient.from_config(config, timeout=30)
        await lite_client.connect()
        try:
            pool_account = await lite_client.get_account_state(pool_address)
        except Exception as ee:
            logger.warning(f"No account state init was found for pool {pool_address_str}, error: {ee}")
            await delete_pool_with_nominators()
            return

    data = pool_account.state.state_init.data

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

    res = (await result_conn.execute(query)).all()

    if res:
        pool_account, pool = res[0]
    else:
        pool_account = Account(
            account=pool_address_str,
            account_type="nominator_pool",
            balance=balance,
        )
        result_conn.add(pool_account)
        await result_conn.flush()
        pool = NominatorPool(account_id=pool_account.account_id)

    pool_account.balance = balance
    pool.validator_amount = validator_amount
    pool.stake_amount_sent = stake_amount_sent
    pool.nominators_count = nominators_count

    pool_id_in_accouts = pool.account_id

    result_conn.add(pool)
    await result_conn.commit()
    logger.info("Updated pool " + pool_address_str)

    # then additional value checks
    if not nominators_cell:
        logger.info("No nominators_cell in pool " + pool_address_str)
        nominators_dict = None
        # await delete_pool_with_nominators()
    else:
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
    # q = q.distinct(Message.created_lt)
    q = q.order_by(Message.created_lt)

    query_msgs_to_pool = q.filter(
        Message.destination == pool_address_str, TransactionMessage.direction == "in"
    )
    # print("query_msgs_to_pool", query_msgs_to_pool)
    query_msgs_from_pool = q.filter(
        Message.source == pool_address_str, TransactionMessage.direction == "out"
    )

    res_to_pool = await origin_conn.execute(query_msgs_to_pool)
    msgs_to_pool = res_to_pool.all()

    res_from_pool = await origin_conn.execute(query_msgs_from_pool)
    msgs_from_pool = res_from_pool.all()

    bookings = []
    withdrawal_requests = {}

    class MsgAndSeqno(NamedTuple):
        msg: Message
        block_seqno: int

    async def process_recover_stake(args: MsgAndSeqno):
        assert (  # came from elector
            args.msg.source
            == "-1:3333333333333333333333333333333333333333333333333333333333333333"
        )

        wc = -1
        shard = -9223372036854775808
        prev_block_seqno = args.block_seqno - 5  # 5 blocks before

        q = select(Block).filter(Block.seqno == prev_block_seqno, Block.workchain == wc)
        res = await origin_conn.execute(q)
        block = res.scalar()
        if not block:
            logger.debug("No block at %s" % prev_block_seqno)
            return

        root_hash = base64.b64decode(block.root_hash)
        file_hash = base64.b64decode(block.file_hash)

        on_block = BlockIdExt(wc, shard, prev_block_seqno, root_hash, file_hash)
        try:
            res = await lite_client.run_get_method(
                pool_address_str, "get_pool_data", [], on_block
            )
            logger.debug(f"Success run get_pool_data on {pool_address_str} on block {on_block.seqno}")
        except:
            logger.error(f"Failed to run get_pool_data on {pool_address_str} on block {on_block.seqno}")
            return

        stake_amount_sent_before = res[2]
        validator_share = res[5]
        nominators_before = res[9]

        if not nominators_before:
            return

        _nominators_dict = HashMap.parse(
            dict_cell=nominators_before.begin_parse(),
            key_length=256,
            key_deserializer=addr_hash_wc0_parse,
            value_deserializer=nominator_value_parse,
        )
        if not _nominators_dict:
            logger.debug("No nominators at %s" % on_block)
            return

        reward = args.msg.value - stake_amount_sent_before
        validator_reward = int(reward * validator_share / 10000)
        nominators_reward = reward - validator_reward

        logger.debug(
            f"Value: {nanostr(args.msg.value)}, stake: {nanostr(stake_amount_sent_before)}, reward: {nanostr(reward)}"
        )
        # logger.debug(f"Block time: {block.gen_utime}, income time: {args.msg.created_at}")

        # logger.debug(
        #     "Found income %s for %s nominators"
        #     % (nanostr(reward), len(_nominators_dict))
        # )
        for nominator, (balance, pending_balance) in _nominators_dict.items():
            share = balance / stake_amount_sent_before
            his_reward = int(share * nominators_reward)
            if not his_reward:
                continue
            bookings.append(
                {
                    "lt": args.msg.created_lt,
                    "utime": args.msg.created_at,
                    "subaccount_address": nominator.to_str(False).upper(),
                    "debit": 0,
                    "credit": his_reward,
                    "type": "nominator_income",
                }
            )
    # end of function

    incomes_to_process = []
    for msg, body, descr, block_seqno in msgs_to_pool:
        # logger.debug(f"     new tx (to) with lt {msg.created_lt} at {msg.created_at}")
        try:
            # bitwise or to catch any other than 0
            exit_code = descr["compute_ph"]["exit_code"]
            action_code = 0
            if "action" in descr:
                action_code = descr["action"]["result_code"]

        except Exception as e:
            logger.debug(
                f"Failed to parse tx description at {msg.created_lt} on {pool_address_str}, {e}"
            )
            continue

        if exit_code != 0 or action_code != 0:
            logger.debug(f"Failed tx: {exit_code} at {msg.created_lt}")
            continue

        body_boc = Cell.from_boc(body)[0].begin_parse()
        try:
            op = body_boc.load_uint(32)
        except:
            continue
        if op == 0:
            try:
                first_letter = chr(body_boc.load_uint(8))[0]
            except:
                continue
            if first_letter == "d":
                bookings.append(
                    {
                        "lt": msg.created_lt,
                        "utime": msg.created_at,
                        "subaccount_address": msg.source,
                        "debit": 0,
                        "credit": msg.value - 10**9,
                        "type": "nominator_deposit",
                    }
                )
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
            incomes_to_process.append(MsgAndSeqno(msg, block_seqno))

    async with aiometer.amap(
        process_recover_stake,
        incomes_to_process,
        max_at_once=2,
        max_per_second=2,
    ) as results:
        pass

    for msg, body, descr, block_seqno in msgs_from_pool:
        # logger.debug(
        #     f"     new tx (from pool) with lt {msg.created_lt} at {msg.created_at}"
        # )
        # pool sends withdrawals in bounceable mode
        if not msg.destination.startswith("0:"):
            continue
        if "bounce" in descr and descr["bounce"] is True:
            logger.debug(f"Bounceable, skip")
            continue
        # and it's more than 1 TON
        if msg.value < 10**9:
            logger.debug(f"Less than 1 TON ({nanostr(msg.value)}), skip")
            continue
        # and without body (empty cell). excesses will have op
        if body != "te6cckEBAQEAAgAAAEysuc0=":
            logger.debug(
                f"Non-empty body (value {nanostr(msg.value)}) (body {body}), skip"
            )
            continue

        if not msg.destination in withdrawal_requests:
            logger.debug(f"No requests from {msg.destination}, skip")
            continue

        # search for withdrawal request max 36 hours ago (2 rounds)
        min_at = msg.created_at - 36 * 3600
        max_at = msg.created_at
        for req_at in withdrawal_requests[msg.destination]:
            if req_at > min_at and req_at <= max_at:
                break
        else:
            logger.debug(f"No requests from {msg.destination} in last 36 hours, skip")
            continue

        # if found:
        logger.info(
            "Found withdrawal msg from pool %s, amount %s, receiver %s at %s"
            % (msg.source, msg.value, msg.destination, msg.created_at)
        )
        bookings.append(
            {
                "lt": msg.created_lt,
                "utime": msg.created_at,
                "subaccount_address": msg.destination,
                "debit": msg.value,
                "credit": 0,
                "type": "nominator_withdrawal",
            }
        )

    # sort bookings by utime and lt
    bookings = sorted(bookings, key=lambda b: (b["utime"], b["lt"]))

    # now we have complete bookings and we'll insert all of them + nominator in db

    # first, create all subaccounts for all nominators
    q = (
        select(SubAccount, Nominator)
        .select_from(SubAccount)
        .join(Nominator, Nominator.subaccount_id == SubAccount.subaccount_id)
        .filter(SubAccount.parent_account_id == pool_id_in_accouts)
    )
    res = await result_conn.execute(q)
    existing_subaccounts = res.all()

    all_subaccounts = {}  # including new below
    for sub in existing_subaccounts:
        all_subaccounts[sub[0].owner] = {
            "subaccount": sub[0],
            "nominator": sub[1],
        }

    # update active nominators
    new_active_nominators = []
    if nominators_dict:
        for nominator, (balance, pending_balance) in nominators_dict.items():
            nominator_raw = nominator.to_str(False).upper()
            if not nominator_raw in all_subaccounts:
                new_subaccount = SubAccount(
                    owner=nominator_raw,
                    subaccount_type="pool_nominator",
                    parent_account_id=pool_id_in_accouts,
                )
                result_conn.add(new_subaccount)
                await result_conn.flush()  # for subaccount_id

                new_nominator = Nominator(
                    subaccount_id=new_subaccount.subaccount_id,
                    balance=balance,
                    pending_balance=pending_balance,
                )
                result_conn.add(new_nominator)  # no flush here

                all_subaccounts[nominator_raw] = {
                    "subaccount": new_subaccount,
                    "nominator": new_nominator,
                }
            else:
                # update data
                all_subaccounts[nominator_raw]["nominator"].balance = balance
                all_subaccounts[nominator_raw][
                    "nominator"
                ].pending_balance = pending_balance

            new_active_nominators.append(nominator.to_str(False).upper())

    # make old active nominators inactive
    for nominator_addr in all_subaccounts:
        if nominator_addr not in new_active_nominators:
            all_subaccounts[nominator_addr]["nominator"].balance = 0
            all_subaccounts[nominator_addr]["nominator"].pending_balance = 0

    # create new nominators
    for record in bookings:
        if not record["subaccount_address"] in all_subaccounts:
            new_subaccount = SubAccount(
                owner=record["subaccount_address"],
                subaccount_type="pool_nominator",
                parent_account_id=pool_id_in_accouts,
            )
            result_conn.add(new_subaccount)
            await result_conn.flush()
            new_nominator = Nominator(
                subaccount_id=new_subaccount.subaccount_id,
                balance=0,
                pending_balance=0,
            )
            result_conn.add(new_nominator)
            all_subaccounts[record["subaccount_address"]] = {
                "subaccount": new_subaccount,
                "nominator": new_nominator,
            }

    # insert bookings and subaccount nominators
    for record in bookings:
        result_conn.add(all_subaccounts[record["subaccount_address"]]["subaccount"])
        result_conn.add(all_subaccounts[record["subaccount_address"]]["nominator"])

        subaccount_id = all_subaccounts[record["subaccount_address"]][
            "subaccount"
        ].subaccount_id

        record["account_id"] = pool_id_in_accouts
        record["subaccount_id"] = subaccount_id
        record_hash = sha256(json.dumps(record, sort_keys=True).encode())
        record_hash = base64.b64encode(record_hash.digest()).decode("ascii")

        res = await result_conn.execute(
            select(Booking).filter_by(booking_hash=record_hash)
        )
        if res.scalar():  # already exists
            continue

        booking = Booking(
            booking_hash=record_hash,
            # account_id=pool_id_in_accouts,
            subaccount_id=subaccount_id,
            booking_lt=record["lt"],
            booking_utime=record["utime"],
            booking_type=record["type"],
            credit=record["credit"],
            debit=record["debit"],
        )
        result_conn.add(booking)

    await result_conn.commit()


nominator_pool_handler = (
    "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=",
    handler,
)
