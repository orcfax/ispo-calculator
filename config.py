import os

env = dict(os.environ)
# START_EPOCH is the first epoch to be snapshotted using the active stake
START_EPOCH = 400
END_EPOCH = 450
POOL_IDS_BECH32 = ['pool10s6zdzdnncnfc200wnlp9endaeud76v424zdnurx9askwshm02x']  # FAX

# rewards rate for the amount of ADA staked (assuming the $FACT token also has 6 decimals)
# rewards = REWARDS_RATE * active_stake
DECIMALS = 6
REWARDS_RATE = 1 / 10

# settings
FILES_PATH = os.getenv('FILES_PATH', 'files')
LOGS_PATH = os.getenv('LOGS_PATH', 'log')
DB_PATH = os.getenv('DB_PATH', 'db')
SNAPSHOT_LOG_FILE = os.getenv('SNAPSHOT_LOG_FILE', LOGS_PATH + '/snapshot.log')
DB_NAME = DB_PATH + '/ispo.db'

API_PORT = 3000
API_LOG_FILE = os.getenv('API', LOGS_PATH + '/api.log')
API_NAME = os.getenv('API_NAME', 'Rewards API')
API_DESCRIPTION = os.getenv('API_DESCRIPTION', 'A simple API for providing the accumulated ISPO rewards')
API_VERSION_MAJOR = os.getenv('API_VERSION', 'v0')
API_VERSION_MINOR = os.getenv('API_VERSION_MINOR', API_VERSION_MAJOR + '.1')
