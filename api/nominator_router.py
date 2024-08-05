import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, FastAPI, Path, Query, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import parse_obj_as
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.exceptions import HTTPException as StarletteHTTPException

from api import crud, schemas
from core.connections import SessionMaker_Result as SessionMaker
from core.settings import Settings
from core.utils import address_to_raw, hash_to_b64, hex_to_int

settings = Settings()
router = APIRouter()


# Dependency
async def get_db():
    async with SessionMaker() as db:
        yield db


@router.get("/getNominator", response_model=List[schemas.NominatorModel])
async def get_nominator_method(
    nominator: str = Query(
        description="The nominator address.",
    ),
    pool: Optional[str] = Query(
        default=None,
        description="The pool address in which nominator stakes coins. If not specified, returns nominator from all his pools.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get nominator data in given pool (the only in list) or, if pool is not specified, in all pools where nominator stakes.
    """
    # check args:
    nominator_addr = address_to_raw(nominator)
    if nominator_addr is None:
        raise HTTPException(status_code=400, detail="Invalid nominator address")
    pool_addr = address_to_raw(pool)

    # get from db
    raw_res = await crud.get_nominator(db, nominator_addr, pool_addr)
    if raw_res is None:
        raise HTTPException(status_code=404, detail="Nominator not found")

    nominators_res = []
    for raw_nominator in raw_res:
        nominators_res.append(
            schemas.NominatorModel(
                pool_address=raw_nominator[0],
                balance=raw_nominator[1],
                pending_balance=raw_nominator[2],
            )
        )
    return nominators_res


@router.get("/getPool", response_model=schemas.NominatorPoolModel)
async def get_pool_method(
    pool: str = Query(
        description="The pool address. Can be sent in hex, base64 or base64url form.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get pool data with all its nominators.
    """
    pool_addr = address_to_raw(pool)
    if pool_addr is None:
        raise HTTPException(status_code=400, detail="Invalid pool address")
    res = await crud.get_pool(db, pool_addr)
    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")
    return res


@router.get("/getNominatorBookings", response_model=List[schemas.BookingMinimalModel])
async def get_nominator_bookings_method(
    nominator: str = Query(description="The nominator address."),
    pool: str = Query(description="Pool address to get bookings in."),
    limit: Optional[int] = Query(
        default=100,
        description="Limit from bottom.",
    ),
    from_time: Optional[int] = Query(
        default=None,
        description="Unixtime of the first booking.",
    ),
    to_time: Optional[int] = Query(
        default=None,
        description="Unixtime of the last booking.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get nominator bookings (debits and credits) in specified pool.
    """

    # check args:
    nominator_addr = address_to_raw(nominator)
    if nominator_addr is None:
        raise HTTPException(status_code=400, detail="Invalid nominator address")

    pool_addr = address_to_raw(pool)
    if pool_addr is None:
        raise HTTPException(status_code=400, detail="Invalid pool address")

    if limit is None:
        limit = 100

    res = await crud.get_nominator_bookings(
        db, nominator_addr, pool_addr, limit, from_time, to_time
    )

    if res is None:
        raise HTTPException(status_code=404, detail="Nominator or pool not found")

    return res


@router.get("/getNominatorEarnings", response_model=schemas.EarningsModel)
async def get_nominator_earnings_method(
    nominator: str = Query(description="The nominator address."),
    pool: str = Query(description="Pool address to get earnings in."),
    limit: Optional[int] = Query(
        default=100,
        description="Limit from bottom.",
    ),
    from_time: Optional[int] = Query(
        default=None,
        description="Unixtime of the first booking.",
    ),
    to_time: Optional[int] = Query(
        default=None,
        description="Unixtime of the last booking.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get nominator income in specified pool with his stake on each timepoint.
    """

    # check args:
    nominator_addr = address_to_raw(nominator)
    if nominator_addr is None:
        raise HTTPException(status_code=400, detail="Invalid nominator address")

    pool_addr = address_to_raw(pool)
    if pool_addr is None:
        raise HTTPException(status_code=400, detail="Invalid pool address")

    if limit is None:
        limit = 100

    # get from db
    res = await crud.get_nominator_earnings(
        db, nominator_addr, pool_addr, limit, from_time, to_time
    )

    if res is None:
        raise HTTPException(status_code=404, detail="Nominator or pool not found")

    return res


@router.get("/getPoolBookings", response_model=List[schemas.BookingModel])
async def get_pool_bookings_method(
    pool: str = Query(description="Pool address to get bookings in."),
    limit: Optional[int] = Query(
        default=100,
        description="Limit from bottom.",
    ),
    from_time: Optional[int] = Query(
        default=None,
        description="Unixtime of the first booking.",
    ),
    to_time: Optional[int] = Query(
        default=None,
        description="Unixtime of the last booking.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all the bookings (debits and credits) in specified pool.
    """

    # check args:
    pool_addr = address_to_raw(pool)
    if pool_addr is None:
        raise HTTPException(status_code=400, detail="Invalid pool address")

    if limit is None:
        limit = 100

    res = await crud.get_pool_bookings(db, pool_addr, limit, from_time, to_time)

    if res is None:
        raise HTTPException(status_code=404, detail="Nominator or pool not found")

    return res
