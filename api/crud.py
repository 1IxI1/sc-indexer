import logging
from dataclasses import dataclass
from typing import List, Optional, Sequence

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Query, Session, aliased, contains_eager, selectinload

from api.schemas import (
    ActiveNominatorModel,
    BookingMinimalModel,
    BookingModel,
    EarningModel,
    EarningsModel,
    NominatorPoolModel,
)
from contracts_db.database import Account, Booking, Nominator, NominatorPool, SubAccount

logger = logging.getLogger(__name__)


async def get_nominator(
    session: AsyncSession,
    address: str,
    pool_address: str | None = None,
):
    """Returns nominator data in pool or in all his pools."""

    query = select(
        Account.account, Nominator.balance, Nominator.pending_balance
    ).select_from(SubAccount)
    query = query.join(Nominator, Nominator.subaccount_id == SubAccount.subaccount_id)
    query = query.join(Account, Account.account_id == SubAccount.parent_account_id)
    query = query.filter(SubAccount.owner == address)
    if pool_address:
        query = query.filter(Account.account == pool_address)
    res = await session.execute(query)
    return res.all()


async def get_pool(
    session: AsyncSession,
    address: str,
) -> Optional[NominatorPoolModel]:
    """Just returns the nominator pool's data."""
    pool_q = (
        select(
            NominatorPool.stake_amount_sent,
            NominatorPool.validator_amount,
            NominatorPool.nominators_count,
        )
        .join(Account, Account.account_id == NominatorPool.account_id)
        .filter(Account.account == address)
    )
    pool_res = await session.execute(pool_q)
    pool = pool_res.first()

    if not pool:
        return None

    stake, validator_amount, nominators_count = pool

    nom_q = select(
        SubAccount.owner, Nominator.balance, Nominator.pending_balance
    ).select_from(SubAccount)
    nom_q = nom_q.join(Nominator, Nominator.subaccount_id == SubAccount.subaccount_id)
    nom_q = nom_q.join(Account, Account.account_id == SubAccount.parent_account_id)
    nom_q = nom_q.filter(Account.account == address)

    nom_res = await session.execute(nom_q)

    active_nominators = []  # (address, balance, pending_balance)
    inactive_nominators = []  # addresses

    for nom in nom_res:
        # balance, pending_balance > 0 - active
        if nom[1] > 0 or nom[2] > 0:
            active_nominators.append(
                ActiveNominatorModel(
                    address=nom[0], balance=nom[1], pending_balance=nom[2]
                )
            )
        else:
            inactive_nominators.append(nom[0])

    return NominatorPoolModel(
        stake_amount_sent=stake,
        validator_amount=validator_amount,
        nominators_count=nominators_count,
        active_nominators=active_nominators,
        inactive_nominators=inactive_nominators,
    )


async def get_nominator_bookings(
    session: AsyncSession,
    nominator_address: str,
    pool_address: str,
    limit: int,
    from_time: Optional[int] = None,
    to_time: Optional[int] = None,
) -> Optional[List[BookingMinimalModel]]:
    """Returns nominator bookings (debits and credits) in specified pool."""

    query = (
        select(
            # FIXME: swap credit and debit after reindex
            Booking.booking_utime,
            Booking.booking_type,
            Booking.credit,
            Booking.debit,
        )
        .select_from(Booking)
        .join(SubAccount, SubAccount.subaccount_id == Booking.subaccount_id)
        .join(Account, Account.account_id == Booking.account_id)
    )
    query = query.filter(SubAccount.owner == nominator_address)
    query = query.filter(Account.account == pool_address)
    query = query.order_by(Booking.booking_utime).limit(limit)
    if from_time:
        query = query.filter(Booking.booking_utime >= from_time)
    if to_time:
        query = query.filter(Booking.booking_utime <= to_time)

    bookings_raw = await session.execute(query)
    res = []
    for i in bookings_raw.all():
        res.append(
            BookingMinimalModel(
                utime=i[0],
                booking_type=i[1],
                debit=i[2],
                credit=i[3],
            )
        )
    return res


async def get_nominator_earnings(
    session: AsyncSession,
    nominator_address: str,
    pool_address: str,
    limit: int,
    from_time: Optional[int] = None,
    to_time: Optional[int] = None,
):
    """Returns nominator income in specified pool with stake on each timepoint."""
    if not from_time:
        from_time = 0
    bookings = await get_nominator_bookings(
        session,
        nominator_address,
        pool_address,
        # assume that income records are at least a half of all bookings (not always true, but ok)
        limit * 2,
        0,
        to_time,
    )
    if not bookings:
        return None

    earnings = []

    total_income = 0
    balance = 0
    for booking in bookings:
        if booking.booking_type == "nominator_income" and booking.utime >= from_time:
            earnings.append(
                EarningModel(
                    utime=booking.utime,
                    income=booking.credit,
                    stake_before=balance,
                )
            )
            total_income += booking.credit

        balance += booking.credit
        balance -= booking.debit

        if len(earnings) >= limit:
            break

    return EarningsModel(total_on_period=total_income, earnings=earnings)


async def get_pool_bookings(
    session: AsyncSession,
    pool_address: str,
    limit: int,
    from_time: Optional[int] = None,
    to_time: Optional[int] = None,
) -> Optional[List[BookingModel]]:
    """Returns all the pool bookings."""

    query = (
        select(
            SubAccount.owner,
            Booking.booking_utime,
            Booking.booking_type,
            Booking.credit,
            Booking.debit,
        )
        .select_from(Booking)
        .join(SubAccount, SubAccount.subaccount_id == Booking.subaccount_id)
        .join(Account, Account.account_id == Booking.account_id)
    )
    query = query.filter(Account.account == pool_address)
    query = query.order_by(Booking.booking_utime).limit(limit)
    if from_time:
        query = query.filter(Booking.booking_utime >= from_time)
    if to_time:
        query = query.filter(Booking.booking_utime <= to_time)

    bookings_raw = await session.execute(query)
    res = []
    for i in bookings_raw.all():
        res.append(
            BookingModel(
                nominator_address=i[0],
                utime=i[1],
                booking_type=i[2],
                debit=i[3],
                credit=i[4],
            )
        )
    return res
