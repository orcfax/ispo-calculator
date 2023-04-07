#!/usr/bin/env python3


from config import *
from koios_api.pool import get_pool_info, get_pool_delegators
import sqlite3

if __name__ == '__main__':
    """
    Connect to the database
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    for pool_id_bech32 in POOL_IDS_BECH32:
        pool_info = get_pool_info(pool_id_bech32)
        sql = "SELECT id FROM pools WHERE pool_id_bech32 = ?"
        cur.execute(sql, (pool_id_bech32,))
        row = cur.fetchone()
        pool_id = row[0]
        sql = "UPDATE pools_stake SET active_stake = ?, live_stake = ? " \
              "WHERE pool_id = ?"
        cur.execute(sql, (int(pool_info[0]['active_stake']), int(pool_info[0]['live_stake']), pool_id))
        conn.commit()
        """
        If live stake is >69M, take a live stake snapshot and save it to the database
        """
        if int(pool_info[0]['live_stake']) >= 69000000000000:
            sql = "SELECT count(*) from live_stake_snapshot"
            cur.execute(sql)
            if cur.fetchone()[0] == 0:
                # take the snapshot
                pool_delegators = get_pool_delegators(pool_id_bech32)
                sql_wallet = "INSERT INTO wallets(stake_address) VALUES(?)"
                sql_snapshot = "INSERT INTO live_stake_snapshot" \
                               "(pool_id, wallet_id, live_stake, active_epoch_no, latest_delegation_tx) " \
                               "VALUES(?, ?, ?, ?, ?)"
                for item in pool_delegators:
                    # find out the wallet id
                    cur.execute("SELECT id FROM wallets WHERE stake_address = ?", (item['stake_address'],))
                    row = cur.fetchone()
                    if not row:
                        # new wallet
                        cur.execute(sql_wallet, (item['stake_address'],))
                        wallet_id = cur.lastrowid
                        conn.commit()
                    else:
                        wallet_id = row[0]
                    # insert the wallet into the live snapshot table
                    cur.execute(sql_snapshot, (pool_id_bech32, wallet_id, item['amount'],
                                               item['active_epoch_no'], item['latest_delegation_tx_hash']))
                    conn.commit()
        conn.close()
