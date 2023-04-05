#!/usr/bin/env python3


from config import *
from koios_api.pool import get_pool_info
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
        cur.execute(sql, (pool_info[0]['active_stake'], pool_info[0]['live_stake'], pool_id))
        conn.commit()
        conn.close()
