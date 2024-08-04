from typing import List, Optional

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship
from sqlalchemy.types import BigInteger, Enum, Integer, String

Base = declarative_base()

WalletVersion = Enum("v1", "v2", "v3r1", "v3r2", "v4r2", name="wallet_version")
AccountType = Enum("wallet", "nominator_pool", "l_pool", name="account_type")
LPoolType = Enum("dedust", "stonfi", name="l_pool_type")
SubAccountType = Enum(
    "pool_nominator", "l_pool_asset", name="subaccount_type"
)  # to be extended
BookingType = Enum(
    "nominator_income", "nominator_deposit", "nominator_withdrawal", name="booking_type"
)


# SCHEME: public


# The actual contracts.
#   The base, which is then referenced
# in other more specific tables such
# as `account_types` and `bookings`.
# Each may have its child accounts - subaccounts.
class Account(Base):
    __tablename__ = "accounts"

    account_id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    account = mapped_column(String)
    account_type = mapped_column(AccountType)
    balance = mapped_column(BigInteger)
    updated_utime = mapped_column(Integer)  # timestamp
    subaccounts = relationship(
        "SubAccount",
        back_populates="account",
        cascade="all, delete-orphan",
    )
    l_pool = relationship(
        "LPool",
        back_populates="account",
        cascade="all, delete-orphan",
    )

    # nominator_pool = relationship(
    #     "NominatorPool",
    #     back_populates="account",
    #     cascade="all, delete-orphan",
    # )


# This table contains list of virtual accounts
# assigned to each account.
#   For example, single account might in theory be
# a nominator for several pools, for each of those
# pools a subaccount will be created with information
# regarding this link.
class SubAccount(Base):
    __tablename__ = "subaccounts"

    subaccount_id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    owner = mapped_column(String)  # address, public key or smth
    subaccount_type = mapped_column(SubAccountType)
    parent_account_id = mapped_column(BigInteger, ForeignKey("accounts.account_id"))
    account = relationship("Account", back_populates="subaccounts")

    # bookings = relationship(
    #     "Booking", back_populates="subaccount", cascade="all, delete-orphan"
    # )
    # nominator = relationship(
    #     "Nominator",
    #     back_populates="subaccount",
    #     cascade="all, delete-orphan",
    # )
    l_pool_asset = relationship(
        "LPoolAsset",
        back_populates="subaccount",
        cascade="all, delete-orphan",
    )


# Where bookings for all the subaccounts
# are stored together.
#   Each change of balance on real or
# virtual (sub) account shall be reflected here as
# an individual booking with credit or debit assigned to it.

# class Booking(Base):
#     __tablename__ = "bookings"

#     booking_id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
#     booking_hash = mapped_column(String, primary_key=True)
#     # account_id = mapped_column(BigInteger, ForeignKey("accounts.account_id"))
#     subaccount_id = mapped_column(BigInteger, ForeignKey("subaccounts.subaccount_id"))
#     booking_type = mapped_column(BookingType)
#     booking_lt = mapped_column(BigInteger)
#     booking_utime = mapped_column(Integer)
#     credit = mapped_column(BigInteger)
#     debit = mapped_column(BigInteger)
#     subaccount = relationship("SubAccount", back_populates="bookings")


# SCHEME: account_types
#   Where the specific data of all defined contracts is stored. Each table contains contracts of some type.
# It’s like a folder for them.


# class NominatorPool(Base):
#     __tablename__ = "nominator_pools"
#     __table_args__ = {"schema": "account_types"}

#     account_id = mapped_column(
#         BigInteger, ForeignKey("accounts.account_id"), primary_key=True
#     )
#     stake_amount_sent = mapped_column(BigInteger)
#     validator_amount = mapped_column(BigInteger)
#     nominators_count = mapped_column(Integer)
#     account = relationship("Account", back_populates="nominator_pool")


class LPool(Base):
    __tablename__ = "l_pools"
    __table_args__ = {"schema": "account_types"}

    account_id = mapped_column(
        BigInteger, ForeignKey("accounts.account_id"), primary_key=True
    )
    pool_type = mapped_column(String)
    account = relationship("Account", back_populates="l_pool")
    assets = relationship(
        "LPoolAsset",
        back_populates="lpool",
        cascade="all, delete-orphan",
        primaryjoin="LPool.account_id == LPoolAsset.subaccount_id",
    )


# SCHEME: subaccount_types
#   The same folder for subaccounts.


# class Nominator(Base):
#     __tablename__ = "nominators"
#     __table_args__ = {"schema": "subaccount_types"}

#     subaccount_id = mapped_column(
#         BigInteger,
#         ForeignKey("subaccounts.subaccount_id"),
#         primary_key=True,
#     )
#     balance = mapped_column(BigInteger)
#     pending_balance = mapped_column(BigInteger)
#     subaccount = relationship("SubAccount", back_populates="nominator")


class LPoolAsset(Base):
    __tablename__ = "l_pool_assets"
    __table_args__ = {"schema": "subaccount_types"}

    subaccount_id = mapped_column(
        BigInteger, ForeignKey("subaccounts.subaccount_id"), primary_key=True
    )
    jetton_symbol = mapped_column(String)
    subaccount = relationship("SubAccount", back_populates="l_pool_asset")
    lpool = relationship(
        "LPool",
        back_populates="assets",
        primaryjoin="LPoolAsset.subaccount_id == LPool.account_id",
    )
