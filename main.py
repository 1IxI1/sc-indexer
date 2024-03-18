import asyncio
import json
import os

from dotenv import load_dotenv
from loguru import logger
from pytoniq.liteclient import LiteClient
from sqlalchemy import select, text

from contracts_db.database import Base as ContractsBase
from core.connections import SessionMaker_Origin, SessionMaker_Result, engine_result
from core.localdb import localdb
from core.processors import call_handler
from core.settings import settings
from processors import handlers as contract_handlers
from mainnet_db.database import LatestAccountState

load_dotenv()

os.makedirs("logs", exist_ok=True)
logger.add("logs/sci_{time}.log", level="DEBUG")

# use LS to get contracts' data
config = json.loads(open("dedicated-config.json").read())
lite_client = LiteClient.from_config(config, timeout=10)

print(contract_handlers.keys())

PERIOD = 5
CHUNK_SIZE = 5


@logger.catch()
async def run():
    localdb.read()

    contract_types = contract_handlers.keys()
    logger.warning(
        f"Start handling {len(contract_types)} contract types from second {localdb.index_second}."
    )

    async with SessionMaker_Origin() as origin_db:
        query = (
            select(
                LatestAccountState.account,
                LatestAccountState.balance,
                LatestAccountState.code_hash,
                LatestAccountState.data_hash,
                LatestAccountState.timestamp,
            )
            .filter(LatestAccountState.code_hash.in_(contract_types))
            .filter(LatestAccountState.timestamp > localdb.index_second)
            .order_by(LatestAccountState.timestamp)
            # .filter( # DEBUG
            #     LatestAccountState.account
            #     == "-1:56CB0E4CDD07AD4A608E0A4F4A5972552139A63A16AA4A620E27056FE9F2C552"
            # )
        )

        res = await origin_db.execute(query)
        all_accounts = res.all()
        logger.warning(f"Found {len(all_accounts)} accounts of described types.")

    tasks = []  # gathering buffer
    processing_timestamp = 0  # in case of error - save chunk's timestamp
    for i, (account, balance, code_hash, data_hash, timestamp) in enumerate(
        all_accounts
    ):
        tasks.append(
            call_handler(
                code_hash,
                SessionMaker_Origin,
                SessionMaker_Result,
                account,
                balance,
                data_hash,
                lite_client,
            )
        )

        if not processing_timestamp:
            processing_timestamp = timestamp - 1

        if len(tasks) >= CHUNK_SIZE or i == len(all_accounts) - 1:
            try:
                pass
            except Exception as e:
                logger.error(
                    "Something went wrong on indexing around timestamp",
                    processing_timestamp,
                )
                logger.error(f"Saving second {localdb.index_second} to local db")
                localdb.index_second = processing_timestamp
                localdb.write()
                processing_timestamp = 0
                raise e
            await asyncio.gather(*tasks)
            tasks = []
            processing_timestamp = 0

    if all_accounts:
        last_timestamp = all_accounts[-1][4]
        localdb.index_second = last_timestamp
        localdb.write()
        logger.warning(
            f"Finished index cycle and saved second {localdb.index_second} to local db"
        )
    else:
        logger.warning(
            f"Finished index cycle. Nothing since the second {localdb.index_second}."
        )


async def main():
    logger.warning("Starting Smart Contracts Indexer")
    async with engine_result.begin() as conn:  # TODO: maybe put into connections
        await conn.run_sync(ContractsBase.metadata.create_all)

    await lite_client.connect()  # LiteClient

    while True:
        await run()
        await asyncio.sleep(PERIOD)


if __name__ == "__main__":
    asyncio.run(main())
