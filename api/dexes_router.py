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


# @router.get("/getNominator", response_model=List[schemas.NominatorModel])
# async def get_nominator_method(
#     nominator: str = Query(
#         description="The nominator address.",
#     ),
#     pool: Optional[str] = Query(
#         default=None,
#         description="The pool address in which nominator stakes coins. If not specified, returns nominator from all his pools.",
#     ),
#     db: AsyncSession = Depends(get_db),
# ):
#     """
#     Get nominator data in given pool (the only in list) or, if pool is not specified, in all pools where nominator stakes.
#     """
#     # check args:
#     nominator_addr = address_to_raw(nominator)
#     if nominator_addr is None:
#         raise HTTPException(status_code=400, detail="Invalid nominator address")
#     pool_addr = address_to_raw(pool)

#     # get from db
#     raw_res = await crud.get_nominator(db, nominator_addr, pool_addr)
#     if raw_res is None:
#         raise HTTPException(status_code=404, detail="Nominator not found")

#     nominators_res = []
#     for raw_nominator in raw_res:
#         nominators_res.append(
#             schemas.NominatorModel(
#                 pool_address=raw_nominator[0],
#                 balance=raw_nominator[1],
#                 pending_balance=raw_nominator[2],
#             )
#         )
#     return nominators_res


@router.get("/getLPool", response_model=schemas.LPoolModel)
async def get_lpool_method(
    lpool: str = Query(
        description="The pool address. Can be sent in hex, base64 or base64url form.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get pool data with all its assets.
    """
    pool_addr = address_to_raw(lpool)
    if pool_addr is None:
        raise HTTPException(status_code=400, detail="Invalid pool address")
    res = await crud.get_lpool(db, pool_addr)
    if not res:
        raise HTTPException(status_code=404, detail="Pool not found")
    return res


@router.get("/getPoolByAsset", response_model=List[schemas.LPoolModel])
async def get_nominator_method(
    asset1: str = Query(
        description="The first asset address (`ton` for TON on dedust).",
    ),
    asset2: Optional[str] = Query(
        default=None,
        description="The second asset address. If not specified, returns all pools with the first asset.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get all liquidity pools for given asset or pair.
    """
    # check args:
    if asset1.lower() == "ton":
        asset1_addr = ""
    else:
        asset1_addr = address_to_raw(asset1)
        if asset1_addr is None:
            raise HTTPException(status_code=400, detail="Invalid asset1 address")

    asset2_addr = None
    if asset2 == "ton":
        asset2_addr = ""
    elif asset2:
        asset2_addr = address_to_raw(asset2)
        if asset2_addr is None:
            raise HTTPException(status_code=400, detail="Invalid asset2 address")

    # get from db
    pools_res = await crud.get_pools_by_assets(db, asset1_addr, asset2_addr)
    if pools_res is None:
        raise HTTPException(status_code=404, detail="Pools not found")

    return pools_res
