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


class Event(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    meta: Mapped[Dict[str, Any]] = mapped_column(JSONB)

    # transactions: List["EventTransaction"] = relationship("EventTransaction", back_populates="event")
    transactions: Mapped[List["Transaction"]] = relationship(
        "Transaction",
        foreign_keys=[id],
        primaryjoin="Event.id == Transaction.event_id",
        uselist=True,
        viewonly=True,
    )
    edges: Mapped[List["EventEdge"]] = relationship("EventEdge", back_populates="event")


# class EventTransaction(Base):
#     __tablename__ = 'event_transactions'
#     event_id: int = mapped_column(BigInteger, ForeignKey("events.id"), primary_key=True)
#     tx_hash: str = mapped_column(String, ForeignKey("transactions.hash"), primary_key=True)

#     event: Event = relationship("Event", back_populates="transactions")
#     transactions: List["Transaction"] = relationship("Transaction", back_populates="event")


class EventEdge(Base):
    __tablename__ = "event_graph"
    event_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("events.id"), primary_key=True
    )
    left_tx_hash: Mapped[str] = mapped_column(String, primary_key=True)
    right_tx_hash: Mapped[str] = mapped_column(String, primary_key=True)

    event: Mapped["Event"] = relationship("Event", back_populates="edges")


class Transaction(Base):
    __tablename__ = "transactions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["block_workchain", "block_shard", "block_seqno"],
            ["blocks.workchain", "blocks.shard", "blocks.seqno"],
        ),
    )

    block_workchain = mapped_column(Integer)
    block_shard = mapped_column(BigInteger)
    block_seqno = mapped_column(Integer)

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

    event_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    account_state_before = relationship(
        "AccountState",
        foreign_keys=[account_state_hash_before],
        primaryjoin="AccountState.hash == Transaction.account_state_hash_before",
        viewonly=True,
    )
    account_state_after = relationship(
        "AccountState",
        foreign_keys=[account_state_hash_after],
        primaryjoin="AccountState.hash == Transaction.account_state_hash_after",
        viewonly=True,
    )
    account_state_latest = relationship(
        "LatestAccountState",
        foreign_keys=[account],
        primaryjoin="LatestAccountState.account == Transaction.account",
        lazy="selectin",
        viewonly=True,
    )
    description = mapped_column(JSONB)

    messages: Mapped[List["TransactionMessage"]] = relationship(
        "TransactionMessage", back_populates="transaction"
    )
    event: Mapped[Optional["Event"]] = relationship(
        "Event",
        foreign_keys=[event_id],
        primaryjoin="Transaction.event_id == Event.id",
        viewonly=True,
    )
    # event: Event = relationship("EventTransaction", back_populates="transactions")


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
    __tablename__ = "messages"
    hash: Mapped[str] = mapped_column(String(44), primary_key=True)
    source: Mapped[str] = mapped_column(String)
    destination: Mapped[str] = mapped_column(String)
    value: Mapped[int] = mapped_column(BigInteger)
    fwd_fee: Mapped[int] = mapped_column(BigInteger)
    ihr_fee: Mapped[int] = mapped_column(BigInteger)
    created_lt: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[int] = mapped_column(BigInteger)
    opcode: Mapped[int] = mapped_column(Integer)
    ihr_disabled: Mapped[bool] = mapped_column(Boolean)
    bounce: Mapped[bool] = mapped_column(Boolean)
    bounced: Mapped[bool] = mapped_column(Boolean)
    import_fee: Mapped[int] = mapped_column(BigInteger)
    body_hash: Mapped[str] = mapped_column(String(44))
    init_state_hash: Mapped[Optional[str]] = mapped_column(String(44), nullable=True)

    transactions = relationship(
        "TransactionMessage",
        foreign_keys=[hash],
        primaryjoin="TransactionMessage.message_hash == Message.hash",
        uselist=True,
        viewonly=True,
    )
    message_content = relationship(
        "MessageContent",
        foreign_keys=[body_hash],
        primaryjoin="Message.body_hash == MessageContent.hash",
        viewonly=True,
    )
    init_state = relationship(
        "MessageContent",
        foreign_keys=[init_state_hash],
        primaryjoin="Message.init_state_hash == MessageContent.hash",
        viewonly=True,
    )

    source_account_state = relationship(
        "LatestAccountState",
        foreign_keys=[source],
        primaryjoin="Message.source == LatestAccountState.account",
        lazy="selectin",
        viewonly=True,
    )

    destination_account_state = relationship(
        "LatestAccountState",
        foreign_keys=[destination],
        primaryjoin="Message.destination == LatestAccountState.account",
        lazy="selectin",
        viewonly=True,
    )


class TransactionMessage(Base):
    __tablename__ = "transaction_messages"
    transaction_hash: Mapped[str] = mapped_column(
        String(44), ForeignKey("transactions.hash"), primary_key=True
    )
    message_hash: Mapped[str] = mapped_column(String(44), primary_key=True)
    direction: Mapped[str] = mapped_column(
        Enum("in", "out", name="direction"), primary_key=True
    )

    transaction: Mapped["Transaction"] = relationship(
        "Transaction", back_populates="messages"
    )
    # message = relationship("Message", back_populates="transactions")
    message: Mapped["Message"] = relationship(
        "Message",
        foreign_keys=[message_hash],
        primaryjoin="TransactionMessage.message_hash == Message.hash",
        viewonly=True,
    )


class MessageContent(Base):
    __tablename__ = "message_contents"

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
# Index("blocks_index_1", Block.workchain, Block.shard, Block.seqno, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "blocks_index_2",
    Block.gen_utime,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "blocks_index_3",
    Block.mc_block_workchain,
    Block.mc_block_shard,
    Block.mc_block_seqno,
    postgresql_using="btree",
    postgresql_concurrently=False,
)

Index(
    "transactions_index_1",
    Transaction.block_workchain,
    Transaction.block_shard,
    Transaction.block_seqno,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "transactions_index_2",
    Transaction.account,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
# Index("transactions_index_3", Transaction.hash, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "transactions_index_3",
    Transaction.now,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "transactions_index_4",
    Transaction.lt,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "transactions_index_6",
    Transaction.event_id,
    postgresql_using="btree",
    postgresql_concurrently=False,
)

# Index('account_states_index_1', AccountState.hash, postgresql_using='btree', postgresql_concurrently=False)
# Index('account_states_index_2', AccountState.code_hash, postgresql_using='btree', postgresql_concurrently=False)

# Index("messages_index_1", Message.hash, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "messages_index_2",
    Message.source,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "messages_index_3",
    Message.destination,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "messages_index_4",
    Message.created_lt,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
# Index("messages_index_5", Message.created_at, postgresql_using='btree', postgresql_concurrently=False)
# Index("messages_index_6", Message.body_hash, postgresql_using='btree', postgresql_concurrently=False)
# Index("messages_index_7", Message.init_state_hash, postgresql_using='btree', postgresql_concurrently=False)

# Index("transaction_messages_index_1", TransactionMessage.transaction_hash, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "transaction_messages_index_2",
    TransactionMessage.message_hash,
    postgresql_using="btree",
    postgresql_concurrently=False,
)

# Index("message_contents_index_1", MessageContent.hash, postgresql_using='btree', postgresql_concurrently=False)

# Index("jetton_wallets_index_1", JettonWallet.address, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "jetton_wallets_index_2",
    JettonWallet.owner,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "jetton_wallets_index_3",
    JettonWallet.jetton,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
# Index("jetton_wallets_index_4", JettonWallet.code_hash, postgresql_using='btree', postgresql_concurrently=False)

# Index("jetton_masters_index_1", JettonMaster.address, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "jetton_masters_index_2",
    JettonMaster.admin_address,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
# Index("jetton_masters_index_3", JettonMaster.code_hash, postgresql_using='btree', postgresql_concurrently=False)

# Index("jetton_transfers_index_1", JettonTransfer.transaction_hash, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "jetton_transfers_index_2",
    JettonTransfer.source,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "jetton_transfers_index_3",
    JettonTransfer.destination,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "jetton_transfers_index_4",
    JettonTransfer.jetton_wallet_address,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
# Index("jetton_transfers_index_5", JettonTransfer.response_destination, postgresql_using='btree', postgresql_concurrently=False)

# Index("jetton_burns_index_1", JettonBurn.transaction_hash, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "jetton_burns_index_2",
    JettonBurn.owner,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "jetton_burns_index_3",
    JettonBurn.jetton_wallet_address,
    postgresql_using="btree",
    postgresql_concurrently=False,
)

# Index("nft_collections_index_1", NFTCollection.address, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "nft_collections_index_2",
    NFTCollection.owner_address,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
# Index("nft_collections_index_3", NFTCollection.code_hash, postgresql_using='btree', postgresql_concurrently=False)

# Index("nft_items_index_1", NFTItem.address, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "nft_items_index_2",
    NFTItem.collection_address,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "nft_items_index_3",
    NFTItem.owner_address,
    postgresql_using="btree",
    postgresql_concurrently=False,
)

# Index("nft_transfers_index_1", NFTTransfer.transaction_hash, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "nft_transfers_index_2",
    NFTTransfer.nft_item_address,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "nft_transfers_index_3",
    NFTTransfer.old_owner,
    postgresql_using="btree",
    postgresql_concurrently=False,
)
Index(
    "nft_transfers_index_4",
    NFTTransfer.new_owner,
    postgresql_using="btree",
    postgresql_concurrently=False,
)


# # event indexes
# Index("event_transaction_index_1", EventTransaction.tx_hash, postgresql_using='btree', postgresql_concurrently=False)
Index(
    "even_detector__transaction_index_1",
    Transaction.lt.asc(),
    postgresql_where=(Transaction.event_id.is_(None)),
    postgresql_using="btree",
    postgresql_concurrently=False,
)


async def test():
    init_database()


if __name__ == "__main__":
    asyncio.run(test())
