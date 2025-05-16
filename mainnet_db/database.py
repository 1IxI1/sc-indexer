import asyncio
import logging
from dataclasses import dataclass
from time import sleep
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    create_engine,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.future import select
from sqlalchemy.orm import (
    Mapped,
    Session,
    declarative_base,
    mapped_column,
    relationship,
    sessionmaker,
)
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy_utils import create_database, database_exists

from core.settings import Settings

logger = logging.getLogger(__name__)

MASTERCHAIN_INDEX = -1
MASTERCHAIN_SHARD = -9223372036854775808

settings = Settings()


# async engine
def get_engine(settings: Settings):
    # logger.critical(settings.origin_dsn)
    engine = create_async_engine(
        settings.origin_dsn,
        pool_size=128,
        max_overflow=24,
        pool_timeout=128,
        echo=False,
    )
    return engine


engine = get_engine(settings)
SessionMaker = async_sessionmaker(bind=engine)


# sync engine
def get_sync_engine(settings: Settings):
    dsn = settings.origin_dsn.replace("+asyncpg", "+psycopg2")
    # logger.critical(dsn)
    engine = create_engine(
        dsn, pool_size=128, max_overflow=24, pool_timeout=128, echo=False
    )
    return engine


sync_engine = get_sync_engine(settings)
SyncSessionMaker = sessionmaker(bind=sync_engine)

# database
Base = declarative_base()
utils_url = str(engine.url).replace("+asyncpg", "")


def init_database(create=False):
    while not database_exists(utils_url):
        if create:
            logger.info("Creating database")
            create_database(utils_url)

            async def create_tables():
                async with engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)

            asyncio.run(create_tables())
        sleep(0.5)


# types
AccountStatus = Enum("uninit", "frozen", "active", "nonexist", name="account_status")


# classes
class Block(Base):
    __tablename__ = "blocks"
    __table_args__ = (
        ForeignKeyConstraint(
            ["mc_block_workchain", "mc_block_shard", "mc_block_seqno"],
            ["blocks.workchain", "blocks.shard", "blocks.seqno"],
        ),
    )

    workchain: Mapped[int] = mapped_column(Integer, primary_key=True)
    shard: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    seqno: Mapped[int] = mapped_column(Integer, primary_key=True)
    root_hash: Mapped[str] = mapped_column(String(44))
    file_hash: Mapped[str] = mapped_column(String(44))

    mc_block_workchain: Mapped[int] = mapped_column(Integer, nullable=True)
    mc_block_shard: Mapped[str] = mapped_column(BigInteger, nullable=True)
    mc_block_seqno: Mapped[int] = mapped_column(Integer, nullable=True)

    masterchain_block = relationship(
        "Block", remote_side=[workchain, shard, seqno], backref="shard_blocks"
    )

    global_id: Mapped[int] = mapped_column(Integer)
    version: Mapped[int] = mapped_column(Integer)
    after_merge: Mapped[bool] = mapped_column(Boolean)
    before_split: Mapped[bool] = mapped_column(Boolean)
    after_split: Mapped[bool] = mapped_column(Boolean)
    want_split: Mapped[bool] = mapped_column(Boolean)
    key_block: Mapped[bool] = mapped_column(Boolean)
    vert_seqno_incr: Mapped[bool] = mapped_column(Boolean)
    flags: Mapped[int] = mapped_column(Integer)
    gen_utime: Mapped[int] = mapped_column(BigInteger)
    start_lt: Mapped[int] = mapped_column(BigInteger)
    end_lt: Mapped[int] = mapped_column(BigInteger)
    validator_list_hash_short: Mapped[int] = mapped_column(Integer)
    gen_catchain_seqno: Mapped[int] = mapped_column(Integer)
    min_ref_mc_seqno: Mapped[int] = mapped_column(Integer)
    prev_key_block_seqno: Mapped[int] = mapped_column(Integer)
    vert_seqno: Mapped[int] = mapped_column(Integer)
    master_ref_seqno: Mapped[int] = mapped_column(Integer, nullable=True)
    rand_seed: Mapped[str] = mapped_column(String(44))
    created_by: Mapped[str] = mapped_column(String)

    tx_count: Mapped[int] = mapped_column(Integer)

    transactions = relationship("Transaction", back_populates="block")


class Transaction(Base):
    __tablename__ = 'transactions'
    __table_args__ = (
        ForeignKeyConstraint(
            ["block_workchain", "block_shard", "block_seqno"],
            ["blocks.workchain", "blocks.shard", "blocks.seqno"]
        ),
    )

    block_workchain = mapped_column(Integer)
    block_shard = mapped_column(BigInteger)
    block_seqno = mapped_column(Integer)
    mc_block_seqno = mapped_column(Integer)
    trace_id = mapped_column(String(44))

    block = relationship("Block", back_populates="transactions")

    account: Mapped[str] = mapped_column(String)
    hash: Mapped[str] = mapped_column(String, primary_key=True)
    lt: Mapped[int] = mapped_column(BigInteger)
    prev_trans_hash = mapped_column(String)
    prev_trans_lt = mapped_column(BigInteger)
    now: Mapped[int] = mapped_column(Integer)

    orig_status = mapped_column(AccountStatus)
    end_status = mapped_column(AccountStatus)

    total_fees = mapped_column(BigInteger)

    account_state_hash_before = mapped_column(String)
    account_state_hash_after = mapped_column(String)

    descr = mapped_column(Enum('ord', 'storage', 'tick_tock', 'split_prepare',
                        'split_install', 'merge_prepare', 'merge_install', name='descr_type'))
    aborted: Mapped[bool] = mapped_column(Boolean)
    destroyed: Mapped[bool] = mapped_column(Boolean)
    credit_first: Mapped[bool] = mapped_column(Boolean)
    is_tock: Mapped[bool] = mapped_column(Boolean)
    installed: Mapped[bool] = mapped_column(Boolean)
    storage_fees_collected: Mapped[int] = mapped_column(BigInteger)
    storage_fees_due: Mapped[int] = mapped_column(BigInteger)
    storage_status_change = mapped_column(Enum('unchanged', 'frozen', 'deleted', name='status_change_type'))
    credit_due_fees_collected: Mapped[int] = mapped_column(BigInteger)
    credit: Mapped[int] = mapped_column(BigInteger)
    compute_skipped: Mapped[bool] = mapped_column(Boolean)
    skipped_reason = mapped_column(Enum('no_state', 'bad_state', 'no_gas', 'suspended', name='skipped_reason_type'))
    compute_success: Mapped[bool] = mapped_column(Boolean)
    compute_msg_state_used: Mapped[bool] = mapped_column(Boolean)
    compute_account_activated: Mapped[bool] = mapped_column(Boolean)
    compute_gas_fees: Mapped[int] = mapped_column(BigInteger)
    compute_gas_used: Mapped[int] = mapped_column(BigInteger)
    compute_gas_limit: Mapped[int] = mapped_column(BigInteger)
    compute_gas_credit: Mapped[int] = mapped_column(BigInteger)
    compute_mode: Mapped[int] = mapped_column(Integer)
    compute_exit_code: Mapped[int] = mapped_column(Integer)
    compute_exit_arg: Mapped[int] = mapped_column(Integer)
    compute_vm_steps: Mapped[int] = mapped_column(BigInteger)
    compute_vm_init_state_hash: Mapped[str] = mapped_column(String)
    compute_vm_final_state_hash: Mapped[str] = mapped_column(String)
    action_success: Mapped[bool] = mapped_column(Boolean)
    action_valid: Mapped[bool] = mapped_column(Boolean)
    action_no_funds: Mapped[bool] = mapped_column(Boolean)
    action_status_change = mapped_column(Enum('unchanged', 'frozen', 'deleted', name='status_change_type'))
    action_total_fwd_fees: Mapped[int] = mapped_column(BigInteger)
    action_total_action_fees: Mapped[int] = mapped_column(BigInteger)
    action_result_code: Mapped[int] = mapped_column(Integer)
    action_result_arg: Mapped[int] = mapped_column(Integer)
    action_tot_actions: Mapped[int] = mapped_column(Integer)
    action_spec_actions: Mapped[int] = mapped_column(Integer)
    action_skipped_actions: Mapped[int] = mapped_column(Integer)
    action_msgs_created: Mapped[int] = mapped_column(Integer)
    action_action_list_hash: Mapped[str] = mapped_column(String)
    action_tot_msg_size_cells: Mapped[int] = mapped_column(BigInteger)
    action_tot_msg_size_bits: Mapped[int] = mapped_column(BigInteger)
    bounce = mapped_column(Enum('negfunds', 'nofunds', 'ok', name='bounce_type'))
    bounce_msg_size_cells: Mapped[int] = mapped_column(BigInteger)
    bounce_msg_size_bits: Mapped[int] = mapped_column(BigInteger)
    bounce_req_fwd_fees: Mapped[int] = mapped_column(BigInteger)
    bounce_msg_fees: Mapped[int] = mapped_column(BigInteger)
    bounce_fwd_fees: Mapped[int] = mapped_column(BigInteger)
    split_info_cur_shard_pfx_len: Mapped[int] = mapped_column(Integer)
    split_info_acc_split_depth: Mapped[int] = mapped_column(Integer)
    split_info_this_addr: Mapped[str] = mapped_column(String)
    split_info_sibling_addr: Mapped[str] = mapped_column(String)

    account_state_before = relationship("AccountState",
                                        foreign_keys=[account_state_hash_before],
                                        primaryjoin="AccountState.hash == Transaction.account_state_hash_before",
                                        viewonly=True)
    account_state_after = relationship("AccountState",
                                       foreign_keys=[account_state_hash_after],
                                       primaryjoin="AccountState.hash == Transaction.account_state_hash_after",
                                       viewonly=True)
    account_state_latest = relationship("LatestAccountState",
                                       foreign_keys=[account],
                                       primaryjoin="LatestAccountState.account == Transaction.account",
                                       lazy='selectin',
                                       viewonly=True)
    messages: Mapped[List["Message"]] = relationship("Message", back_populates="transaction", viewonly=True)
    trace: Mapped[Optional["Trace"]] = relationship("Trace", foreign_keys=[trace_id], primaryjoin="Transaction.trace_id == Trace.trace_id", viewonly=True)
    emulated: Mapped[bool] = mapped_column(Boolean)

class Trace(Base):
    __tablename__ = 'traces'
    trace_id = mapped_column(String(44), primary_key=True)
    external_hash: Mapped[str] = mapped_column(String)
    mc_seqno_start: Mapped[int] = mapped_column(Integer)
    mc_seqno_end: Mapped[int] = mapped_column(Integer)
    start_lt: Mapped[int] = mapped_column(BigInteger)
    start_utime: Mapped[int] = mapped_column(Integer)
    end_lt: Mapped[int] = mapped_column(BigInteger)
    end_utime: Mapped[int] = mapped_column(Integer)
    state = mapped_column(Enum('complete', 'pending', 'broken', name='trace_state'))
    pending_edges_: Mapped[int] = mapped_column(BigInteger)
    edges_: Mapped[int] = mapped_column(BigInteger)
    nodes_: Mapped[int] = mapped_column(BigInteger)
    classification_state = mapped_column(Enum('unclassified', 'failed', 'ok', 'broken', name='trace_classification_state'))

    transactions: Mapped[List["Transaction"]] = relationship("Transaction",
                                                     foreign_keys=[trace_id],
                                                     primaryjoin='Trace.trace_id == Transaction.trace_id',
                                                     uselist=True,
                                                     viewonly=True)

class AccountState(Base):
    __tablename__ = "account_states"

    hash = mapped_column(String, primary_key=True)
    account = mapped_column(String)
    balance = mapped_column(BigInteger)
    account_status = mapped_column(
        Enum("uninit", "frozen", "active", name="account_status_type")
    )
    frozen_hash = mapped_column(String)
    code_hash = mapped_column(String)
    data_hash = mapped_column(String)


class Message(Base):
    __tablename__ = 'messages'
    msg_hash: Mapped[str] = mapped_column(String(44), primary_key=True)
    tx_hash: Mapped[str] = mapped_column(String(44), ForeignKey("transactions.hash"), primary_key=True)
    tx_lt: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    direction = mapped_column(Enum('out', 'in', name='msg_direction'), primary_key=True)
    trace_id: Mapped[str] = mapped_column(String(44))
    source: Mapped[str] = mapped_column(String)
    destination: Mapped[str] = mapped_column(String)
    value: Mapped[int] = mapped_column(BigInteger)
    fwd_fee: Mapped[int] = mapped_column(BigInteger)
    ihr_fee: Mapped[int] = mapped_column(BigInteger)
    created_lt: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[int] = mapped_column(BigInteger)
    opcode: Mapped[int] = mapped_column(BigInteger)
    ihr_disabled: Mapped[bool] = mapped_column(Boolean)
    bounce: Mapped[bool] = mapped_column(Boolean)
    bounced: Mapped[bool] = mapped_column(Boolean)
    import_fee: Mapped[int] = mapped_column(BigInteger)
    body_hash: Mapped[str] = mapped_column(String(44))
    init_state_hash: Mapped[Optional[str]] = mapped_column(String(44), nullable=True)
    value_extra_currencies: Mapped[dict] = mapped_column(JSONB, nullable=True)

    transaction = relationship("Transaction",
                               viewonly=True,
                               back_populates="messages",
                               foreign_keys=[tx_hash],
                               primaryjoin="Message.tx_hash == Transaction.hash")

    message_content = relationship("MessageContent",
                                   foreign_keys=[body_hash],
                                   primaryjoin="Message.body_hash == MessageContent.hash",
                                   viewonly=True)
    init_state = relationship("MessageContent",
                              foreign_keys=[init_state_hash],
                              primaryjoin="Message.init_state_hash == MessageContent.hash",
                              viewonly=True)

    source_account_state = relationship("LatestAccountState",
                              foreign_keys=[source],
                              primaryjoin="Message.source == LatestAccountState.account",
                              lazy='selectin',
                              viewonly=True)

    destination_account_state = relationship("LatestAccountState",
                              foreign_keys=[destination],
                              primaryjoin="Message.destination == LatestAccountState.account",
                              lazy='selectin',
                              viewonly=True)

    def __repr__(self):
        opcode = self.opcode
        if opcode is not None:
            if opcode > 0:
                opcode = hex(opcode)
            else:
                opcode = hex(opcode & 0xffffffff)

        return f"Message({self.direction}, {self.msg_hash}, {opcode})"


class MessageContent(Base):
    __tablename__ = 'message_contents'

    hash: Mapped[str] = mapped_column(String(44), primary_key=True)
    body: Mapped[str] = mapped_column(String)

    # message = relationship("Message", back_populates="message_content")
class JettonWallet(Base):
    __tablename__ = "jetton_wallets"
    address = mapped_column(String, primary_key=True)
    balance: Mapped[int] = mapped_column(Numeric)
    owner = mapped_column(String)
    jetton = mapped_column(String)
    last_transaction_lt = mapped_column(BigInteger)
    code_hash = mapped_column(String)
    data_hash = mapped_column(String)

    transfers: Mapped[List["JettonTransfer"]] = relationship(
        "JettonTransfer",
        foreign_keys=[address],
        primaryjoin="JettonWallet.address == JettonTransfer.jetton_wallet_address",
        viewonly=True,
    )
    burns: Mapped[List["JettonBurn"]] = relationship(
        "JettonBurn",
        foreign_keys=[address],
        primaryjoin="JettonWallet.address == JettonBurn.jetton_wallet_address",
        viewonly=True,
    )

    jetton_master: Mapped["JettonMaster"] = relationship(
        "JettonMaster",
        foreign_keys=[jetton],
        primaryjoin="JettonWallet.jetton == JettonMaster.address",
    )


class JettonMaster(Base):
    __tablename__ = "jetton_masters"
    address = mapped_column(String, primary_key=True)
    total_supply: Mapped[int] = mapped_column(Numeric)
    mintable: Mapped[bool] = mapped_column(Boolean)
    admin_address = mapped_column(String, nullable=True)
    jetton_content = mapped_column(JSONB, nullable=True)
    jetton_wallet_code_hash = mapped_column(String)
    code_hash = mapped_column(String)
    data_hash = mapped_column(String)
    last_transaction_lt = mapped_column(BigInteger)
    code_boc = mapped_column(String)
    data_boc = mapped_column(String)


class JettonTransfer(Base):
    __tablename__ = "jetton_transfers"
    transaction_hash = mapped_column(
        String, ForeignKey("transactions.hash"), primary_key=True
    )
    query_id: Mapped[int] = mapped_column(Numeric)
    amount: Mapped[int] = mapped_column(Numeric)
    source = mapped_column(String)
    destination = mapped_column(String)
    jetton_wallet_address = mapped_column(String)
    response_destination = mapped_column(String)
    custom_payload = mapped_column(String)
    forward_ton_amount: Mapped[int] = mapped_column(Numeric)
    forward_payload = mapped_column(String)

    transaction: Mapped[Transaction] = relationship("Transaction")
    jetton_wallet: Mapped[JettonWallet] = relationship(
        "JettonWallet",
        foreign_keys=[jetton_wallet_address],
        primaryjoin="JettonWallet.address == JettonTransfer.jetton_wallet_address",
    )


class JettonBurn(Base):
    __tablename__ = "jetton_burns"
    transaction_hash = mapped_column(
        String, ForeignKey("transactions.hash"), primary_key=True
    )
    query_id: Mapped[int] = mapped_column(Numeric)
    owner: Mapped[str] = mapped_column(String)
    jetton_wallet_address: Mapped[str] = mapped_column(String)
    amount: Mapped[int] = mapped_column(Numeric)
    response_destination = mapped_column(String)
    custom_payload = mapped_column(String)

    transaction: Mapped[Transaction] = relationship("Transaction")
    jetton_wallet: Mapped[JettonWallet] = relationship(
        "JettonWallet",
        foreign_keys=[jetton_wallet_address],
        primaryjoin="JettonWallet.address == JettonBurn.jetton_wallet_address",
    )


class NFTCollection(Base):
    __tablename__ = "nft_collections"
    address = mapped_column(String, primary_key=True)
    next_item_index: Mapped[int] = mapped_column(Numeric)
    owner_address = mapped_column(String)
    collection_content = mapped_column(JSONB)
    data_hash = mapped_column(String)
    code_hash = mapped_column(String)
    last_transaction_lt = mapped_column(BigInteger)
    code_boc = mapped_column(String)
    data_boc = mapped_column(String)

    items: Mapped[List["NFTItem"]] = relationship(
        "NFTItem",
        foreign_keys=[address],
        primaryjoin="NFTCollection.address == NFTItem.collection_address",
    )


class NFTItem(Base):
    __tablename__ = "nft_items"
    address = mapped_column(String, primary_key=True)
    init: Mapped[bool] = mapped_column(Boolean)
    index: Mapped[int] = mapped_column(Numeric)
    collection_address = mapped_column(String)  # TODO: index
    owner_address = mapped_column(String)  # TODO: index
    content = mapped_column(JSONB)
    last_transaction_lt = mapped_column(BigInteger)
    code_hash = mapped_column(String)
    data_hash = mapped_column(String)

    collection: Mapped[Optional[NFTCollection]] = relationship(
        "NFTCollection",
        foreign_keys=[collection_address],
        primaryjoin="NFTCollection.address == NFTItem.collection_address",
    )

    transfers: Mapped[List["NFTTransfer"]] = relationship(
        "NFTTransfer",
        foreign_keys=[address],
        primaryjoin="NFTItem.address == NFTTransfer.nft_item_address",
    )


class NFTTransfer(Base):
    __tablename__ = "nft_transfers"
    transaction_hash = mapped_column(
        String, ForeignKey("transactions.hash"), primary_key=True
    )
    query_id: Mapped[int] = mapped_column(Numeric)
    nft_item_address = mapped_column(String)  # TODO: index
    old_owner = mapped_column(String)  # TODO: index
    new_owner = mapped_column(String)  # TODO: index
    response_destination = mapped_column(String)
    custom_payload = mapped_column(String)
    forward_amount: Mapped[int] = mapped_column(Numeric)
    forward_payload = mapped_column(String)

    transaction: Mapped[Transaction] = relationship("Transaction")
    nft_item: Mapped[NFTItem] = relationship(
        "NFTItem",
        foreign_keys=[nft_item_address],
        primaryjoin="NFTItem.address == NFTTransfer.nft_item_address",
    )


class LatestAccountState(Base):
    __tablename__ = "latest_account_states"
    account = mapped_column(String, primary_key=True)
    hash = mapped_column(String)
    code_hash = mapped_column(String)
    data_hash = mapped_column(String)
    frozen_hash = mapped_column(String)
    account_status = mapped_column(String)
    timestamp = mapped_column(Integer)
    last_trans_lt = mapped_column(BigInteger)
    balance: Mapped[int] = mapped_column(Numeric)


# Indexes
# Index("blocks_index_1", Block.workchain, Block.shard, Block.seqno)
Index("blocks_index_2", Block.gen_utime)
Index("blocks_index_3", Block.mc_block_workchain, Block.mc_block_shard, Block.mc_block_seqno)
Index("blocks_index_4", Block.seqno, postgresql_where=(Block.workchain == -1))
Index("blocks_index_5", Block.start_lt)

Index("transactions_index_1", Transaction.block_workchain, Transaction.block_shard, Transaction.block_seqno)
Index("transactions_index_2", Transaction.account, Transaction.lt)
Index("transactions_index_2a", Transaction.account, Transaction.now)
Index("transactions_index_3", Transaction.now, Transaction.hash)
Index("transactions_index_4", Transaction.lt, Transaction.hash)
Index("transactions_index_8", Transaction.mc_block_seqno)

# Index('account_states_index_1', AccountState.hash)
# Index('account_states_index_2', AccountState.code_hash)

# Index("messages_index_1", Message.hash)
Index("messages_index_2", Message.source)
Index("messages_index_3", Message.destination)
Index("messages_index_4", Message.created_lt)
# Index("messages_index_5", Message.created_at)
# Index("messages_index_6", Message.body_hash)
# Index("messages_index_7", Message.init_state_hash)

# Index("transaction_messages_index_1", TransactionMessage.transaction_hash, postgresql_using='btree', postgresql_concurrently=False)
# Index("message_contents_index_1", MessageContent.hash, postgresql_using='btree', postgresql_concurrently=False)

# Index("jetton_wallets_index_1", JettonWallet.address)
Index("jetton_wallets_index_2", JettonWallet.owner)
Index("jetton_wallets_index_3", JettonWallet.jetton)
Index("jetton_wallets_index_4", JettonWallet.jetton, JettonWallet.balance)
# Index("jetton_wallets_index_4", JettonWallet.code_hash)

# Index("jetton_masters_index_1", JettonMaster.address)
Index("jetton_masters_index_2", JettonMaster.admin_address)
# Index("jetton_masters_index_3", JettonMaster.code_hash)

# Index("jetton_transfers_index_1", JettonTransfer.transaction_hash)
Index("jetton_transfers_index_2", JettonTransfer.source)
Index("jetton_transfers_index_3", JettonTransfer.destination)
Index("jetton_transfers_index_4", JettonTransfer.jetton_wallet_address)
# Index("jetton_transfers_index_5", JettonTransfer.response_destination)

# Index("jetton_burns_index_1", JettonBurn.transaction_hash)
Index("jetton_burns_index_2", JettonBurn.owner)
Index("jetton_burns_index_3", JettonBurn.jetton_wallet_address)

# Index("nft_collections_index_1", NFTCollection.address)
Index("nft_collections_index_2", NFTCollection.owner_address)
# Index("nft_collections_index_3", NFTCollection.code_hash)

# Index("nft_items_index_1", NFTItem.address)
Index("nft_items_index_2", NFTItem.collection_address)
Index("nft_items_index_3", NFTItem.owner_address)
Index("nft_items_index_4", NFTItem.collection_address, NFTItem.index)

# Index("nft_transfers_index_1", NFTTransfer.transaction_hash)
Index("nft_transfers_index_2", NFTTransfer.nft_item_address)
Index("nft_transfers_index_3", NFTTransfer.old_owner)
Index("nft_transfers_index_4", NFTTransfer.new_owner)

async def test():
    init_database()


if __name__ == "__main__":
    asyncio.run(test())
