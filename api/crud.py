import logging
from typing import List, Optional, Sequence

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Query, Session, aliased, contains_eager, selectinload

from contracts_db.database import Booking, Nominator, NominatorPool

logger = logging.getLogger(__name__)


async def get_nominator(
    session: AsyncSession,
    address: str,
    pool_address: str | None,
) -> Sequence[Nominator]:
    """Returns nominator data in pool or in all his pools."""
    query = select(Nominator).join(NominatorPool)
    query = query.filter(Nominator.account == address)
    if pool_address:
        query = query.filter(NominatorPool.account == pool_address)
    res = await session.execute(query)
    return res.scalars().all()


async def get_nominator_bookings(
    session: AsyncSession,
    nominator_address: str,
    pool_address: str | None,
    limit: int = 100,
    limit_from_top: bool = False,
) -> Sequence[Booking]:
    """Returns nominator data in pool or in all his pools."""

    query = select(Booking).join(Nominator)
    query = query.filter(Nominator.account == nominator_address)

    if pool_address:
        query = query.filter(NominatorPool.account == pool_address)

    if limit_from_top:
        query = query.order_by(Booking.id.desc())
    else:
        query = query.order_by(Booking.id)
    query = query.limit(limit)
    res = await session.execute(query)
    return res.scalars().all()


async def get_pool(
    session: AsyncSession,
    address: str,
) -> Optional[NominatorPool]:
    """Just returns the nominator pool's data."""
    query = (
        select(NominatorPool)
        .filter(NominatorPool.account == address)
        .options(selectinload(NominatorPool.nominators))
    )
    res = await session.execute(query)
    return res.scalar()
