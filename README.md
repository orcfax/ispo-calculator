# ispo-calculator
An app that lets Orcfax ISPO delegators calculate their rewards. It makes data available via an API. 
The [`ispo-calculator-ui`](https://github.com/orcfax/ispo-calculator-ui) app provides a web front-end to this calculator.


## The backend
### config.py
This is the configuration file. The following variables must be defined:
- POOL_IDS_BECH32: stake pool ID(s) (as a list with one or more stake pool IDs in bech32 format).
- START_EPOCH: the epoch when the ISPO starts.
- END_EPOCH: the epoch when the ISPO ends. It can be set to a value in the future and changed later.
- REWARDS_RATE: the amount of tokens won as rewards per epoch for each ADA staked.
- FILES_PATH: the path where the files will be saved. It can be set also as an environment variable.
- LOGS_PATH: the path where the log file(s) will be saved. It can be set also as an environment variable.
- DB_PATH: the path where the database will be saved. It can be set also as an environment variable.
- SNAPSHOT_LOG_FILE: the snapshot log file
- DB_NAME: the sqlite3 database file

### library.py
This file has a few functions used by the main script. It also creates the required folders and sets up logging.
The database structure:

![OrcFax ISPO Database](doc/fax_ispo_database.png)

### snapshot.py
This is the main script which must be executed to do the stake snapshot for each epoch and to calculate the rewards.
It can be executed once each epoch, but it can also be executed for multiple epochs from the past.
The script also calculates if the snapshots for the previous epochs are correct, and if not, it will do the snapshot 
again for them. At the end, the ISPO data will be written in a JSON file and in an Excel file, and the snapshots 
will be validated one more time, comparing the total active stake of the pools for each epoch with the sum of the 
active stake of all delegators for each epoch.
The script is using the Koios API via the [koios_api](https://github.com/cardano-apexpool/koios-api-python) 
Python wrapper.

## The API
### api.py
Endpoints:
- /api/v0/get_rewards/{stake_address}

Accepts a stake address as a parameter. Returns the rewards amount (as string, with decimals), for each epoch, 
the total amount of rewards (as string, with decimals) and the stake address.
```json
{
  "active_stake": "58,939,667",
  "bonus": "0.0000",
  "ispo_total_adjusted_rewards": "18,800,318.910442",
  "ispo_total_base_rewards": "18,799,730.656773",
  "ispo_total_bonus": "588.2537",
  "latest_epoch": "404",
  "live_stake": "67,437,922",
  "rewards": [
    {
      "active_stake": "1,007,480.254474",
      "adjusted_rewards": "100,748.025447",
      "base_rewards": "100,748.025447",
      "bonus": "0.0000",
      "epoch": "401"
    },
    {
      "active_stake": "1,007,874.485949",
      "adjusted_rewards": "100,787.448594",
      "base_rewards": "100,787.448594",
      "bonus": "0.0000",
      "epoch": "402"
    },
    {
      "active_stake": "1,008,387.301918",
      "adjusted_rewards": "100,838.730191",
      "base_rewards": "100,838.730191",
      "bonus": "0.0000",
      "epoch": "403"
    },
    {
      "active_stake": "1,008,393.994201",
      "adjusted_rewards": "100,839.399420",
      "base_rewards": "100,839.399420",
      "bonus": "0.0000",
      "epoch": "404"
    }
  ],
  "rewards_percentage_from_total": "2.1447168294%",
  "stake_address": "stake1....",
  "total_adjusted_rewards": "403,213.603652",
  "total_base_rewards": "403,213.6037",
  "total_bonus": "0.0000",
  "total_ispo_rewards_percent": "18.80%"
}
```
- /api/v0/get_total_rewards/

Returns the total rewards accumulated by all wallets, including the rewards for the current epoch.
```json
{
  "active_stake": "58939667.95319",
  "adjusted_rewards": "18800318.9104418",
  "base_rewards": "18799730.656773",
  "bonus": "588.2537",
  "latest_epoch": "404",
  "live_stake": "67437922.194257"
}
```
