import logging
from typing import List, Optional, Sequence

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Query, Session, aliased, contains_eager, selectinload

from contracts_db.database import Nominator, NominatorPool

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
