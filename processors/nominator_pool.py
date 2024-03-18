from loguru import logger
from pytoniq.liteclient import LiteClient
from pytoniq_core.boc import Address, Cell
from pytoniq_core.boc.hashmap import HashMap
from sqlalchemy import and_, delete, or_, select
from sqlalchemy.orm import selectinload

from contracts_db.database import Nominator, NominatorPool
from core.handler_types import DBSession
from core.utils import addr_hash_wc0_parse, empty_parse, nanostr, nominator_value_parse
from mainnet_db.database import Message, MessageContent, Transaction, TransactionMessage


# @SC_handler(code_hash="mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=")
async def handle_nominator_pool(
    origin_db: DBSession,
    result_db: DBSession,
    pool_address_str: str,
    balance: int,
    data_hash: str,
    lite_client: LiteClient,
):
    """
    Nominator pool contract.
    https://github.com/ton-blockchain/nominator-pool/
    """
    async with origin_db() as origin_conn, result_db() as result_conn:

        # use it to clean data if pool cannot be processed
        async def delete_pool():
            query = delete(Nominator).where(Nominator.pool_address == pool_address_str)
            await result_conn.execute(query)
            query = delete(NominatorPool).where(
                NominatorPool.account == pool_address_str
            )
            await result_conn.execute(query)

        pool_address = Address(pool_address_str)
        try:
            account = await lite_client.get_account_state(pool_address)
            if not account.state.state_init or not account.state.state_init.data:
                raise Exception("No data in state init")
        except Exception:
            logger.info("No account state init was found for pool " + pool_address_str)
            await delete_pool()
            return

        data = account.state.state_init.data

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
        if not nominators_cell:
            logger.info("No nominators_cell in pool " + pool_address_str)
            await delete_pool()
            return

        nominators_dict = HashMap.parse(
            dict_cell=nominators_cell.begin_parse(),
            key_length=256,
            key_deserializer=addr_hash_wc0_parse,
            value_deserializer=nominator_value_parse,
        )

        if not nominators_dict:
            logger.info("Invalid nominators_dict in pool " + pool_address_str)
            await delete_pool()
            return

        withdraw_requests_cell = ds.load_maybe_ref()
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
                await delete_pool()
                return

        nominators_raw = []

        logger.debug("\n========== Pool: " + pool_address.to_str() + " ==========\n")

        # go for nominators and count their deposit
        for nominator_address, (balance, pending_balance) in nominators_dict.items():

            nominator_addr_raw = nominator_address.to_str(False).upper()

            q = (
                select(Message, MessageContent.body, Transaction.description)
                .select_from(Message)
                .join(MessageContent, Message.body_hash == MessageContent.hash)
                .join(
                    TransactionMessage, Message.hash == TransactionMessage.message_hash
                )
                .join(
                    Transaction, TransactionMessage.transaction_hash == Transaction.hash
                )
            )
            q = q.filter(
                or_(
                    and_(
                        Message.source == nominator_addr_raw,
                        Message.destination == pool_address_str,
                        Message.opcode == 0,
                        Transaction.account == Message.destination,
                    )
                    | and_(
                        Message.source == pool_address_str,
                        Message.destination == nominator_addr_raw,
                    )
                )
            )
            q = q.order_by(Message.source, Message.created_lt)

            res = await origin_conn.execute(q)

            msgs_and_other_all = res.all()

            msgs_from_nominator = []
            msgs_from_pool = []

            for msg, body, descr in msgs_and_other_all:
                if msg.source == nominator_addr_raw:
                    msgs_from_nominator.append((msg, body, descr))
                else:
                    msgs_from_pool.append((msg, body, descr))
            logger.debug(
                f"total: {len(msgs_from_nominator)}, "
                f"msgs from: {nominator_addr_raw}, to: {pool_address_str}"
            )

            # temp variable. when withdraw - all deposits
            # before the withdraw_request's execution won't be accounted
            withdrawaw_at_lt = 0
            deposit = 0
            withdrawn_rewards = 0
            for msg, body, descr in msgs_from_nominator:
                logger.debug(
                    f"     new tx with lt {msg.created_lt} at {msg.created_at}"
                )
                body_boc = Cell.from_boc(body)[0].begin_parse().skip_bits(32)
                try:
                    exit_code = descr["action"]["result_code"]
                except:
                    logger.debug(f"Failed to parse tx description: {descr}")
                    continue
                if body_boc.remaining_bits < 8 or exit_code != 0:
                    logger.debug("Invalid message body")
                    continue
                first_letter = chr(body_boc.load_uint(8))[0]

                if first_letter == "d":
                    if (
                        msg.created_lt < withdrawaw_at_lt
                    ):  # withdrawal request already in progress
                        logger.debug(
                            f"Ommiting deposit at lt {msg.created_lt} because the withdrawal lt is {withdrawaw_at_lt}"
                        )

                        # it's not reward.
                        # it's just a deposit wich was returned with the withdrawal
                        withdrawn_rewards -= msg.value
                        continue
                    logger.debug(f"Deposit {nanostr(msg.value)} at {msg.created_lt}")

                    deposit += msg.value - 10**9  # -1 TON fee

                elif first_letter == "w":

                    # look for withdrawal message after this lt
                    for pool_to_usr_msg, body, descr in msgs_from_pool:
                        if (
                            # goes strictly after by LT
                            msg.created_lt < pool_to_usr_msg.created_lt
                            # significant outgoing amount
                            and pool_to_usr_msg.value >= deposit - (2 * 10**9)
                        ):
                            withdrawaw_at_lt = pool_to_usr_msg.created_lt
                            withdrawn_amount = pool_to_usr_msg.value
                            logger.info(
                                f"Found withdrawal of {nanostr(withdrawn_amount)} at {msg.created_lt}"
                            )
                            break
                    else:
                        logger.info(
                            f"Found withdrawal request for nominator {nominator_addr_raw}, but no withdrawal found"
                        )
                        continue

                    reward = withdrawn_amount - deposit
                    withdrawn_rewards += reward
                    deposit = 0

            unwithdrawn_reward = balance - deposit
            if pending_balance > 0:
                logger.debug("Found pending_balance")
                unwithdrawn_reward += pending_balance

            logger.info(" --- Nominator: " + nominator_address.to_str() + " ---")
            logger.info("Balance: " + nanostr(balance))
            logger.info("Deposit: " + nanostr(deposit))
            logger.info("Pending: " + nanostr(pending_balance))
            logger.info("Withdrawn rewards: " + nanostr(withdrawn_rewards))
            logger.info("Unwithdrawn rewards: " + nanostr(unwithdrawn_reward))
            logger.info("")

            nominators_raw.append(
                {
                    "account": nominator_addr_raw,
                    "balance": balance,
                    "deposit": deposit,
                    "withdrawn_rewards": withdrawn_rewards,
                    "total_rewards": withdrawn_rewards + unwithdrawn_reward,
                }
            )

        # inserting data
        pool_res = await result_conn.execute(
            select(NominatorPool)
            .filter_by(account=pool_address_str)
            .options(selectinload(NominatorPool.nominators))
        )
        nominator_pool = pool_res.scalar()
        if not nominator_pool:
            nominator_pool = NominatorPool(
                account=pool_address_str,
                stake_amount_sent=stake_amount_sent,
                validator_amount=validator_amount,
                nominators_count=nominators_count,
            )
        else:
            nominator_pool.stake_amount_sent = stake_amount_sent
            nominator_pool.validator_amount = validator_amount
            nominator_pool.nominators_count = nominators_count

        nominators = []
        for nominator_raw in nominators_raw:
            nominator = await result_conn.get(
                Nominator,
                {
                    "account": nominator_raw["account"],
                    "pool_address": pool_address_str,
                },
            )
            if not nominator:
                nominator = Nominator(
                    account=nominator_raw["account"],
                    pool=nominator_pool,
                    pool_address=pool_address_str,
                    balance=nominator_raw["balance"],
                    deposit=nominator_raw["deposit"],
                    withdrawn_rewards=nominator_raw["withdrawn_rewards"],
                    total_rewards=nominator_raw["total_rewards"],
                )
            else:
                nominator.balance = nominator_raw["balance"]
                nominator.deposit = nominator_raw["deposit"]
                nominator.withdrawn_rewards = nominator_raw["withdrawn_rewards"]
                nominator.total_rewards = nominator_raw["total_rewards"]

            nominators.append(nominator)

        nominator_pool.nominators = nominators

        # TODO: first delete all nominators in this pool?

        result_conn.add(nominator_pool)

        await result_conn.commit()


nominator_pool_handler = (
    "mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=",
    handle_nominator_pool,
)
