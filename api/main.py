import logging

from fastapi import Depends, FastAPI, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from api.deps.apikey import api_key_dep
from api.dexes_router import router as router_dexes
from api.nominator_router import router as router_nominators
from core.settings import Settings

logging.basicConfig(format="%(asctime)s %(module)-15s %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


settings = Settings()
description = "TON Smart Contracts Indexer. Nominator pools, V2."
app = FastAPI(
    title="TON SC Indexer V2" if not settings.api_title else settings.api_title,
    description=description,
    version="0.0.2",
    # root_path=settings.api_root_path,
    docs_url=settings.api_root_path + "/",
    openapi_url=settings.api_root_path + "/openapi.json",
    dependencies=[Depends(api_key_dep)],
)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse({"error": str(exc.detail)}, status_code=exc.status_code)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse({"error": str(exc)}, status_code=status.HTTP_400_BAD_REQUEST)


@app.exception_handler(Exception)
def generic_exception_handler(request, exc):
    return JSONResponse(
        {"error": str(exc)}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE
    )


@app.on_event("startup")
def startup():
    logger.info("Service started successfully")


app.include_router(
    router_nominators,
    prefix=settings.api_root_path,
    include_in_schema=True,
    deprecated=True,
)

app.include_router(
    router_dexes,
    prefix=settings.api_root_path,
    include_in_schema=True,
    deprecated=False,
)
