from typing import List, Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship
from sqlalchemy.types import BigInteger, Enum, Integer, String

Base = declarative_base()

WalletVersion = Enum("v1", "v2", "v3r1", "v3r2", "v4r2", name="wallet_version")


class Wallet(Base):
    __tablename__ = "wallets"

    account = mapped_column(String, primary_key=True)
    public_key = mapped_column(String)
    version = mapped_column(WalletVersion)
    balance = mapped_column(BigInteger)

    # only for some versions
    subwallet_id = mapped_column(Integer, nullable=True)
    seqno = mapped_column(BigInteger, nullable=True)
    # TODO plugins


class NominatorPool(Base):
    __tablename__ = "nominator_pools"

    account = mapped_column(String, primary_key=True)
    stake_amount_sent = mapped_column(BigInteger)
    validator_amount = mapped_column(BigInteger)
    nominators_count = mapped_column(Integer)
    nominators: Mapped[List["Nominator"]] = relationship(
        "Nominator",
        back_populates="pool",
        cascade="all, delete-orphan",
    )


class Nominator(Base):
    __tablename__ = "nominators"

    account = mapped_column(String, primary_key=True)

    pool = relationship("NominatorPool", back_populates="nominators")

    pool_address = mapped_column(
        String, ForeignKey("nominator_pools.account"), primary_key=True
    )

    balance = mapped_column(BigInteger)
    deposit = mapped_column(BigInteger)
    withdrawn_rewards = mapped_column(BigInteger)
    total_rewards = mapped_column(BigInteger)
