import logging
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel

from contracts_db.database import Nominator, NominatorPool

logger = logging.getLogger(__name__)


class LifecheckModel(BaseModel):
    status: str
    last_booking_time: int


class NominatorModel(BaseModel):
    pool_address: str
    balance: int
    pending_balance: int


class ActiveNominatorModel(BaseModel):
    address: str
    balance: int
    pending_balance: int


class NominatorPoolModel(BaseModel):
    stake_amount_sent: int
    validator_amount: int
    nominators_count: int
    active_nominators: List[ActiveNominatorModel]
    inactive_nominators: List[str]


class BookingMinimalModel(BaseModel):
    utime: int
    booking_type: str
    debit: int
    credit: int

class BookingModel(BaseModel):
    nominator_address: str
    utime: int
    booking_type: str
    debit: int
    credit: int

class EarningModel(BaseModel):
    utime: int
    income: int
    stake_before: int

class EarningsModel(BaseModel):
    total_on_period: int
    earnings: List[EarningModel]


# class WalletModel(BaseModel):
#     account: str
#     public_key: str
#     balance: int
#     version: str
#     subwallet_id: Optional[int]
#     seqno: Optional[int]
