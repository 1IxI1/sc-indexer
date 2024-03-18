import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Body, Depends, FastAPI, Path, Query, status
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
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


@router.get("/getNominator", response_model=List[schemas.Nominator])
async def get_nominator_method(
    nominator: str = Query(
        description="The nominator address. Can be sent in hex, base64 or base64url form.",
    ),
    pool: Optional[str] = Query(
        default=None,
        description="The pool address in which nominator stakes coins. If not specified, returns nominator from all his pools. Can be sent in hex, base64 or base64url form.",
    ),
    db: AsyncSession = Depends(get_db),
):
    """
    Get nominator data in given pool (the only in list) or, if pool is not specified, in all pools where nominator stakes.
    """
    nominator_addr = address_to_raw(nominator)
    if nominator_addr is None:
        raise HTTPException(status_code=400, detail="Invalid nominator address")
    pool_addr = address_to_raw(pool)
    res = await crud.get_nominator(db, nominator_addr, pool_addr)
    if res is None:
        raise HTTPException(status_code=404, detail="Nominator not found")
    return res


@router.get("/getPool", response_model=schemas.NominatorPool)
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
    pool = await crud.get_pool(db, pool_addr)
    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")
    return schemas.NominatorPool.model_validate(pool, from_attributes=True)
