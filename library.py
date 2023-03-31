from config import *
import pandas as pd
from koios_api.network import get_tip
from koios_api.epoch import get_epoch_info
from koios_api.pool import get_pool_info
from datetime import datetime
import os
import logging

"""
Create the folders if they don't exist, and create the .gitignore files i
"""
try:
    if not os.path.isdir(FILES_PATH):
        os.makedirs(FILES_PATH)
        with open(FILES_PATH + '/.gitignore', 'w') as f:
            f.write('*')
    if not os.path.isdir(LOGS_PATH):
        os.makedirs(LOGS_PATH)
        with open(LOGS_PATH + '/.gitignore', 'w') as f:
            f.write('*')
    if not os.path.isdir(DB_PATH):
        os.makedirs(DB_PATH)
        with open(DB_PATH + '/.gitignore', 'w') as f:
            f.write('*')
except Exception as e:
    print(e)
    print('cannot create the required directories, exiting!')
    exit(1)

logging.basicConfig(filename=SNAPSHOT_LOG_FILE, format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)


def write_excel_file(total_deleg_per_epoch, excel_file_name='delegators_per_epoch.xlsx'):
    """
    Write the Excel file
    """
    excel_writer = pd.ExcelWriter(FILES_PATH + '/' + excel_file_name, engine='xlsxwriter')
    for ep in total_deleg_per_epoch:
        df = pd.DataFrame(total_deleg_per_epoch[ep])
        df.to_excel(excel_writer, sheet_name=str(ep))
    excel_writer.close()


def epoch_data(db_conn, db_cur, epoch):
    """
    Get the epoch data and save it in the database if not already there
    :param db_conn: the database connection
    :param db_cur: the database cursor
    :param epoch: the epoch number
    :return: the epoch id in the database and the start timestamp
    """
    sql = "SELECT id, start_time FROM epochs WHERE number = ?"
    db_cur.execute(sql, (epoch,))
    row = db_cur.fetchone()
    if not row:
        logging.debug('Adding epoch %d information into the database' % epoch)
        epoch_info = []
        while not epoch_info:
            epoch_info = get_epoch_info(epoch)
        sql = "INSERT INTO epochs (number, start_time, end_time) VALUES (?, ?, ?)"
        db_cur.execute(sql, (epoch, datetime.fromtimestamp(epoch_info[0]['start_time']),
                             datetime.fromtimestamp(epoch_info[0]['end_time'])))
        db_conn.commit()
        epoch_id = db_cur.lastrowid
        start_timestamp = epoch_info[0]['start_time']
    else:
        logging.debug('Epoch %d information already exists in the database' % epoch)
        epoch_id = row[0]
        start_timestamp = datetime.fromisoformat(row[1])
    return epoch_id, start_timestamp


def pool_data(db_conn, db_cur, pool_id_bech32):
    """
    Get the pool data for a pool_id_bech32
    :param db_conn: the database connection
    :param db_cur: the database cursor
    :param pool_id_bech32: the bech32 pool id
    :return: id of the pool in the pools table and the pool ticker
    """
    sql = "SELECT id, ticker FROM pools WHERE pool_id_bech32 = ?"
    db_cur.execute(sql, (pool_id_bech32,))
    row = db_cur.fetchone()
    if not row:
        pool_info = []
        while not pool_info:
            pool_info = get_pool_info(pool_id_bech32)
        try:
            ticker = pool_info[0]['meta_json']['ticker']
        except TypeError:
            ticker = ''
        logging.debug('Adding pool %s (%s) into the database' % (pool_id_bech32, ticker))
        sql = "INSERT INTO pools(pool_id_bech32, ticker) VALUES (?, ?)"
        db_cur.execute(sql, (pool_id_bech32, ticker))
        db_conn.commit()
        pool_id = db_cur.lastrowid
    else:
        logging.debug('Pool %s already exists in the database' % pool_id_bech32)
        pool_id = row[0]
        ticker = row[1]
    return pool_id, ticker


def create_database(db_conn, db_cur):
    """
    Create the tables in the database
    :param db_conn: the database connection
    :param db_cur: the database cursor
    :return: the epochs and the pools dictionaries
    """
    tip = get_tip()
    current_epoch = tip[0]['epoch_no']
    """
    Epochs table
    Information about each epoch, one record per epoch
    """
    db_cur.execute('''CREATE TABLE IF NOT EXISTS epochs (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                number INTEGER NOT NULL,
                start_time timestamp,
                end_time timestamp
                )''')
    db_cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS epochs_number ON epochs(number)''')

    epochs = {}
    for epoch in range(START_EPOCH, current_epoch + 1):
        if epoch > END_EPOCH:
            break
        epoch_id, start_timestamp = epoch_data(db_conn, db_cur, epoch)
        epochs[epoch] = {'id': epoch_id, 'start_timestamp': start_timestamp}

    """
    Pools table
    Information about the stake pools, one record per pool
    """
    db_cur.execute('''CREATE TABLE IF NOT EXISTS pools (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                pool_id_bech32 CHAR(56) NOT NULL,
                ticker CHAR(5)
                )''')
    db_cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS pools_pool_id_bech32 ON pools(pool_id_bech32)''')

    pools = {}
    for pool_id_bech32 in POOL_IDS_BECH32:
        pool_id, ticker = pool_data(db_conn, db_cur, pool_id_bech32)
        pools[pool_id_bech32] = {'id': pool_id, 'ticker': ticker}

    """
    Pools epochs table
    Total delegation for each pool in each epoch, one row per epoch per pool
    """
    db_cur.execute('''CREATE TABLE IF NOT EXISTS pools_epochs (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                pool_id INTEGER NOT NULL,
                epoch_id INTEGER NOT NULL,
                delegators_count INTEGER,
                total_active_stake INTEGER NOT NULL
                )''')

    """
    Wallets table
    Stake address and payment address for each wallet, one record per wallet
    """
    db_cur.execute('''CREATE TABLE IF NOT EXISTS wallets (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                stake_address CHAR(59) NOT NULL
                )''')
    db_cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS wallets_stake_address ON wallets(stake_address)''')

    """
    Wallet history table
    Stake address delegation history, one record per wallet per epoch
    """
    db_cur.execute('''CREATE TABLE IF NOT EXISTS wallets_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                wallet_id INTEGER NOT NULL,
                epoch_id INTEGER NOT NULL,
                pool_id INTEGER NOT NULL,
                active_stake INTEGER NOT NULL,
                epochs_delegated INTEGER NOT NULL DEFAULT 1,
                base_rewards INTEGER NOT NULL DEFAULT 0,
                adjusted_rewards INTEGER NOT NULL DEFAULT 0,
                submitted INTEGER NOT NULL DEFAULT 0
                )''')
    db_cur.execute('''CREATE INDEX IF NOT EXISTS wallets_history_wallet_id ON wallets_history(wallet_id)''')
    db_cur.execute('''CREATE INDEX IF NOT EXISTS wallets_history_epoch_id ON wallets_history(epoch_id)''')

    db_cur.execute('''CREATE TABLE IF NOT EXISTS wallets_addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                wallet_id INTEGER NOT NULL,
                payment_address CHAR(103) NOT NULL
                )''')
    db_cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS wallets_addresses_p_a ON wallets_addresses(payment_address)''')

    return epochs, pools
