# API Methods for nominator accounting

The data is collected.

Now, we have to decide on API for providing clients with this data.

### getNominator

**Arguments:**

- `nominator_address: string`
- `pool_address: string (optional)` - if not provided - all pools where nominator has ever stake in

_Returns the list of pools. One pool in list when address is specified._

```json
[
  {
    "pool_address": "string",
    "balance": 0,
    "pending_balance": 0
  }
]
```

### getNominatorBookings

**Arguments:**

- `nominator_address: string`
- `pool_address: string`
- `limit: number`
- `limit_from_top: boolean` - if _true_, returns not the _first N_ records, but _N last_ records.
- `from_time: number`
- `to_time: number`

_Returns nominator bookings (debits and credits) in specified pool._

### getPool

**Arguments:**

- `pool_address: string`

_Returns pool data (stake amount, validator amount etc) + just nominator list with balances. No accounting._

### getPoolBookings

**Arguments:**

- `pool_address: string`
- `limit: number`
- `limit_from_top: boolean` - if _true_, returns not the _first N_ records, but _N last_ records.
- `from_time: number`
- `to_time: number`

_Returns all pool’s nominators’ accounting records. All their deposits, incomes, withdrawa_
