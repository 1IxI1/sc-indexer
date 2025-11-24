import base64
import logging
from datetime import datetime
from typing import List, Optional

import httpx
from fastapi import APIRouter, Body, Depends, FastAPI, Path, Query, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from pydantic import parse_obj_as
from pytoniq_core.boc import Address, Cell, Slice
from pytoniq_core.boc.hashmap import HashMap
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.exceptions import HTTPException as StarletteHTTPException

from api import crud, schemas
from core.connections import SessionMaker_Result as SessionMaker
from core.settings import Settings
from core.utils import address_to_raw, addr_hash_wc0_parse, hash_to_b64, hex_to_int
from handlers.new_nominator_pool import parse_pool

settings = Settings()
router = APIRouter()

# toncenter api v3 settings
TONCENTER_API_URL = "https://toncenter.com/api/v3"


# Dependency
async def get_db():
    async with SessionMaker() as db:
        yield db


def nominator_value_parse(src: Slice) -> tuple[int, int]:
    # nominator#_ deposit:Coins pending_deposit:Coins = Nominator;
    deposit = src.load_coins() or 0
    pending_deposit = src.load_coins() or 0
    return deposit, pending_deposit


async def get_account_state_from_toncenter(address: str) -> Cell | None:
    """fetch account state from toncenter api v3"""
    url = f"{TONCENTER_API_URL}/accountStates"
    headers = {"X-API-Key": settings.toncenter_api_key}
    params = {"address": address, "include_boc": "true"}
    
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                logging.warning(f"Toncenter API returned status {resp.status_code} for {address}")
                return None
            
            data = resp.json()
            if not data.get("account_states") or len(data["account_states"]) == 0:
                return None
            
            account_state = data["account_states"][0]
            if account_state.get("account_status") != "active":
                return None
            
            # get data boc
            data_boc = account_state.get("data_boc")
            if not data_boc:
                return None
            
            # parse boc
            data_cell = Cell.from_boc(base64.b64decode(data_boc))[0]
            return data_cell
    except Exception as e:
        logging.error(f"Error fetching account state from toncenter for {address}: {e}")
        return None


async def get_pool_data_from_toncenter(pool_address: str) -> tuple | None:
    """
    get pool data from toncenter api v3
    returns parsed pool data tuple or None if failed
    """
    data_cell = await get_account_state_from_toncenter(pool_address)
    if data_cell is None:
        return None
    
    try:
        return parse_pool(data_cell)
    except Exception as e:
        logging.error(f"Failed to parse pool data for {pool_address}: {e}")
        return None


@router.get("/lifecheck", response_model=schemas.LifecheckModel)
async def lifecheck_method(
    db: AsyncSession = Depends(get_db),
):
    """
    Check if there are any bookings in the last 19 hours.
    Returns status "ok" if bookings exist, "dead" otherwise.
    """
    last_booking_time = await crud.get_last_booking(db)
    seconds = 19 * 3600
    current_time = int(datetime.now().timestamp())
    check_time = current_time - seconds

    return schemas.LifecheckModel(
        status="ok" if last_booking_time > check_time else "dead",
        last_booking_time=last_booking_time
    )



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

    # get pools from db
    raw_res = await crud.get_nominator(db, nominator_addr, pool_addr)
    if raw_res is None or len(raw_res) == 0:
        raise HTTPException(status_code=404, detail="Nominator not found")

    nominators_res = []
    for raw_nominator in raw_res:
        pool_address = raw_nominator[0]
        
        # fetch actual balance from toncenter
        pool_data = await get_pool_data_from_toncenter(pool_address)
        if pool_data is None:
            # fallback to db balance if can't get state
            nominators_res.append(
                schemas.NominatorModel(
                    pool_address=pool_address,
                    balance=raw_nominator[1],
                    pending_balance=raw_nominator[2],
                )
            )
            continue
        
        (
            state,
            nominators_count,
            stake_amount_sent,
            validator_amount,
            config,
            nominators_cell,
            withdraw_requests_cell,
        ) = pool_data
        
        # find our nominator in the pool state
        balance = 0
        pending_balance = 0
        if nominators_cell:
            nominators_dict = HashMap.parse(
                dict_cell=nominators_cell.begin_parse(),
                key_length=256,
                key_deserializer=addr_hash_wc0_parse,
                value_deserializer=nominator_value_parse,
            )
            if nominators_dict:
                nominator_address_obj = Address(nominator_addr)
                if nominator_address_obj in nominators_dict:
                    balance, pending_balance = nominators_dict[nominator_address_obj]
        
        nominators_res.append(
            schemas.NominatorModel(
                pool_address=pool_address,
                balance=balance,
                pending_balance=pending_balance,
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
    
    # get pool info from db first to check if it exists
    res = await crud.get_pool(db, pool_addr)
    if not res:
        raise HTTPException(status_code=404, detail="Pool not found")
    
    # now fetch actual balances from toncenter
    pool_data = await get_pool_data_from_toncenter(pool_addr)
    if pool_data is None:
        # fallback to db data if can't get state
        return res
    
    (
        state,
        nominators_count,
        stake_amount_sent,
        validator_amount,
        config,
        nominators_cell,
        withdraw_requests_cell,
    ) = pool_data
    
    # parse nominators from state
    active_nominators = []
    if nominators_cell:
        nominators_dict = HashMap.parse(
            dict_cell=nominators_cell.begin_parse(),
            key_length=256,
            key_deserializer=addr_hash_wc0_parse,
            value_deserializer=nominator_value_parse,
        )
        if nominators_dict:
            for nominator_addr, (balance, pending_balance) in nominators_dict.items():
                active_nominators.append(
                    schemas.ActiveNominatorModel(
                        address=nominator_addr.to_str(False).upper(),
                        balance=balance,
                        pending_balance=pending_balance,
                    )
                )
    
    # get all known nominators from db to find inactive ones
    all_nominators_from_db = {nom.address for nom in res.active_nominators}.union(set(res.inactive_nominators))
    active_addrs = {nom.address for nom in active_nominators}
    inactive_nominators = list(all_nominators_from_db - active_addrs)
    
    return schemas.NominatorPoolModel(
        stake_amount_sent=stake_amount_sent or 0,
        validator_amount=validator_amount or 0,
        nominators_count=nominators_count,
        active_nominators=active_nominators,
        inactive_nominators=inactive_nominators,
    )


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
