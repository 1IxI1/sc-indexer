import base64

from loguru import logger
from pytoniq.liteclient import LiteClient
from sqlalchemy import delete, select

from contracts_db.database import Wallet
from handlers.handler_types import DBSession


async def handle_wallet_v3(
    origin_db: DBSession,
    result_db: DBSession,
    addr: str,
    balance: int,
    data_hash: str,
    lite_client: LiteClient,
):
    async with result_db() as result_conn:

        async def delete_wallet():
            query = delete(Wallet).filter_by(account=addr)
            await result_conn.execute(query)

        find_res = await result_conn.execute(select(Wallet).filter_by(account=addr))
        wallet = find_res.scalar()

        try:
            account = await lite_client.get_account_state(addr)
            if not account.state.state_init or not account.state.state_init.data:
                raise Exception("No data in state init")
        except Exception:
            logger.info("No account state init was found for wallet " + addr)
            await delete_wallet()
            return

        data = account.state.state_init.data

        # wallet#_ seqno:uint32 subwallet:uint32 public_key:bits256 = Storage;
        ds = data.begin_parse()
        seqno = ds.load_uint(32)
        subwallet_id = ds.load_uint(32)
        public_key = ds.load_bytes(32)

        public_key = base64.b64encode(public_key).decode()
        # logger.debug(f"Wallet V3R2 {addr} has public key {public_key}")

        if not wallet:
            wallet = Wallet(
                account=addr,
                public_key=public_key,
                subwallet_id=subwallet_id,
                balance=balance,
                version="v3r2",
                seqno=seqno,
            )
        else:
            wallet.balance = balance
            wallet.seqno = seqno

        result_conn.add(wallet)
        await result_conn.commit()


wallet_v3r2_handler = ("hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=", handle_wallet_v3)
