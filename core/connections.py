from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .settings import settings

engine_origin = create_async_engine(
    url=settings.origin_dsn,
    echo=False,
    pool_size=8,
)

engine_result = create_async_engine(
    url=settings.result_dsn,
    echo=False,
    pool_size=20,
)

SessionMaker_Result = async_sessionmaker(bind=engine_result)
SessionMaker_Origin = async_sessionmaker(bind=engine_origin)

__all__ = ["SessionMaker_Origin", "SessionMaker_Result"]
