import logging
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field
from pytonlib.utils import tlb

from contracts_db.database import Nominator, NominatorPool
from core.utils import address_to_friendly, address_to_raw, b64_to_hex, int_to_hex

logger = logging.getLogger(__name__)


def hash_type(value):
    return b64_to_hex(value).upper() if value else None


def address_type(value: Union[str, None]):
    res = address_to_raw(value)
    if not res:
        return "addr_none"
    return res.upper()


def is_wallet(code_hash):
    wallets_code_hashes = {
        "oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=",  # wallet_v1_r1
        "1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=",  # wallet_v1_r2
        "WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=",  # wallet_v1_r3
        "XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=",  # wallet_v2_r1
        "/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=",  # wallet_v2_r2
        "thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=",  # wallet_v3_r1
        "hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=",  # wallet_v3_r2
        "ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=",  # wallet_v4_r1
        "/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=",  # wallet_v4_r2
    }
    return code_hash in wallets_code_hashes


def address_type_friendly(address_raw, latest_account_state):
    """
    As per address update proposal https://github.com/ton-blockchain/TEPs/pull/123
    we use non-bounceable user-friendly format for nonexist/uninit account and wallets
    and bounceable for others.
    """
    bounceable = True
    if latest_account_state is None:
        # We consider this as destroyed account (nonexist)
        bounceable = False
    elif latest_account_state.account_status == "uninit":
        bounceable = False
    elif is_wallet(latest_account_state.code_hash):
        bounceable = False
    return (
        address_to_friendly(address_raw, bounceable)
        if address_raw and address_raw != "addr_none"
        else None
    )


def shard_type(value):
    return int_to_hex(value, length=64, signed=True).upper() if value else None


class Nominator(BaseModel):
    account: str
    pool_address: str
    balance: int
    deposit: int
    withdrawn_rewards: int
    total_rewards: int


class NominatorPool(BaseModel):
    account: str
    stake_amount_sent: int
    validator_amount: int
    nominators_count: int
    nominators: List["Nominator"]


class Wallet(BaseModel):
    account: str
    public_key: str
    balance: int
    version: str
    subwallet_id: Optional[int]
    seqno: Optional[int]
