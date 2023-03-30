#!/usr/bin/env python3


from library import *
from koios_api.pool import get_pool_history, get_pool_delegators_history
import sqlite3
import json

if __name__ == '__main__':
    """
    Get the current epoch from the API
    """
    tip = get_tip()
    logging.info(f"Current tip: {tip}")
    current_epoch = tip[0]['epoch_no']
    logging.info(f"Current epoch: {current_epoch}")
    epochs_info = {}
    if current_epoch not in epochs_info:
        epoch_info = []
        while not epoch_info:
            """
            This should not be a while loop, one call should be enough,
            but I noticed some empty responses in the past, so I did this as a workaround
            """
            logging.debug(f"calling get_epoch_info({current_epoch})")
            epoch_info = get_epoch_info(current_epoch)
            epochs_info[current_epoch] = epoch_info

    """
    Connect to the database
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    """
    Create the database and populate it with the epochs and pools information 
    """
    epochs, pools = create_database(conn, cur)

    """
    Save the active stake for each pool for each epoch in a variable to be able to double check
    if the active stake for each pool from the database matches the active stake reported by the API
    """
    pools_history = {}
    for epoch in epochs:
        pools_history[epoch] = {}
    for pool_id in pools:
        logging.info(f"Getting pool history for {pools[pool_id]['ticker']}")
        pool_history = get_pool_history(pool_id)
        for item in pool_history:
            if item['epoch_no'] in epochs:
                pools_history[item['epoch_no']][pool_id] = {
                    'active_stake': int(item['active_stake']),
                    'delegators_cnt': item['delegator_cnt']
                }

    """
    Create the list of delegators for each epoch grouped by pool for all epochs 
    """
    total_delegations_per_epoch = {}
    delegators_per_epoch = {}
    total_delegators = 0
    for pool_id in pools:
        for epoch in range(START_EPOCH, current_epoch + 1):
            if epoch > END_EPOCH:
                break
            """
            Get the epoch_id from the database
            or insert the epoch if not in the database
            """
            cur.execute("SELECT id FROM epochs WHERE number = ?", (epoch,))
            row = cur.fetchone()
            epoch_id = row[0]
            logging.info(f"Pool {pools[pool_id]['ticker']:5s} epoch {epoch}")
            total_delegations_per_epoch[epoch] = {}
            total_delegations_per_epoch[epoch][pool_id] = 0
            if epoch not in delegators_per_epoch:
                delegators_per_epoch[epoch] = {}
            delegators_per_epoch[epoch][pool_id] = {}
            # the list of active delegators for the "epoch" epoch
            pool_delegators = get_pool_delegators_history(pool_id, epoch)
            for deleg in pool_delegators:
                total_delegations_per_epoch[epoch][pool_id] += int(deleg['amount'])
                delegators_per_epoch[epoch][pool_id][deleg['stake_address']] = {
                    'active_stake': int(deleg['amount'])
                }
            # total delegation for each pool for the "epoch" epoch
            total_delegation = 0
            total_delegators += len(delegators_per_epoch[epoch][pool_id])
            cur.execute("SELECT count(*) FROM pools_epochs WHERE epoch_id = ? and pool_id = ?",
                        (epoch_id, pools[pool_id]['id']))
            records_count = cur.fetchone()[0]
            if records_count == 0:
                logging.debug(f"Adding pools epochs information for epoch {epoch} into the database")
                cur.execute("INSERT INTO pools_epochs(pool_id, epoch_id, delegators_count, total_active_stake) "
                            "VALUES (?, ?, ?, ?)",
                            (pools[pool_id]['id'], epoch_id, len(delegators_per_epoch[epoch][pool_id]),
                             total_delegations_per_epoch[epoch][pool_id]))
                conn.commit()
            logging.debug(f"Delegation for pool {pools[pool_id]['ticker']:5s} epoch {epoch} "
                          f"from {len(delegators_per_epoch[epoch][pool_id]):5d} "
                          f"delegators: {total_delegations_per_epoch[epoch][pool_id]:15d} lovelace")

    """
    Save the information about all delegators in the database
    """
    wallets = {}
    total_delegators_per_epoch = {}
    sorted_epochs = list(delegators_per_epoch)
    sorted_epochs.sort()
    for epoch in sorted_epochs:
        total_delegators_in_epoch = 0
        already_snapshotted = 0
        for pool_id in delegators_per_epoch[epoch]:
            total_delegators_in_epoch += len(delegators_per_epoch[epoch][pool_id])
            logging.debug(f"{pool_id} ({pools[pool_id]['ticker']:5s}) -> "
                          f"{len(delegators_per_epoch[epoch][pool_id])} delegators")
        logging.debug(f"Epoch {epoch}, total delegators: {total_delegators_in_epoch}")
        total_delegators_per_epoch[epoch] = []
        for pool_id in pools:
            logging.debug(pools[pool_id]['ticker'])
            """
            get the pool information from the database
            get the wallets_history records count for this epoch and this pool
            to see if the script has already run or not
            """
            cur.execute("SELECT count(*) AS delegators_cnt "
                        "FROM wallets_history wh "
                        "JOIN epochs e on e.id = wh.epoch_id "
                        "WHERE wh.pool_id = ? AND e.number = ?", (pools[pool_id]['id'], epoch))
            row = cur.fetchone()
            delegators_cnt = row[0]
            try:
                if delegators_cnt == len(delegators_per_epoch[epoch][pool_id]):
                    logging.debug(f"Snapshot already done for pool {pools[pool_id]['ticker']:5s} epoch {epoch}")
                    already_snapshotted += delegators_cnt
                    continue
                else:
                    logging.debug(f"Delegators count so far: "
                                  f"{delegators_cnt} / {len(delegators_per_epoch[epoch][pool_id])}")
            except Exception as e:
                logging.error(e)
                print(e)
                exit(1)
            for delegator in delegators_per_epoch[epoch][pool_id]:
                cur.execute("SELECT id FROM wallets WHERE stake_address = ?", (delegator,))
                row = cur.fetchone()
                if not row:
                    cur.execute("INSERT INTO wallets(stake_address) VALUES (?)", (delegator,))
                    wallet_id = cur.lastrowid
                    conn.commit()
                else:
                    wallet_id = row[0]
                wallets[delegator] = {'id': wallet_id}
                active_stake = delegators_per_epoch[epoch][pool_id][delegator]['active_stake']
                # Calculate the number of delegated epochs for the current wallet
                cur.execute("SELECT count(*) FROM wallets_history WHERE wallet_id = ?", (wallet_id,))
                epochs_delegated = 1 + cur.fetchone()[0]
                # calculate the rewards based on the active stake and bonuses for multiple epochs delegated
                if epochs_delegated >= 50:
                    BONUS = 1.5
                elif epochs_delegated >= 25:
                    BONUS = 1.3
                elif epochs_delegated >= 10:
                    BONUS = 1.2
                elif epochs_delegated >= 5:
                    BONUS = 1.1
                else:
                    BONUS = 1
                base_rewards = int(active_stake * REWARDS_RATE)
                adjusted_rewards = int(active_stake * REWARDS_RATE * BONUS)
                record = {
                    'Stake Addr': delegator,
                    'Active Stake': active_stake,
                    'Rewards': adjusted_rewards
                }
                total_delegators_per_epoch[epoch].append(record)
                logging.debug(f"[ {epoch} - {pools[pool_id]['ticker']} ] "
                              f"{len(total_delegators_per_epoch[epoch]) + already_snapshotted} / "
                              f"{total_delegators_in_epoch} {delegator}")
                """
                Adjust the past rewards for the current wallet if it reached a new level of bonus
                """
                if epochs_delegated == 5 or epochs_delegated == 10 or epochs_delegated == 25 or epochs_delegated == 50:
                    cur.execute("UPDATE wallets_history SET adjusted_rewards = base_rewards * ? "
                                "WHERE wallet_id = ?", (BONUS, wallet_id))
                conn.commit()
                """
                Insert record into wallets_history if not already there
                or update it in case it is already present
                """
                epoch_id = epochs[epoch]['id']
                cur.execute("SELECT id FROM wallets_history WHERE wallet_id = ? and epoch_id = ?",
                            (wallet_id, epoch_id))
                row = cur.fetchone()
                if not row:
                    cur.execute("INSERT INTO wallets_history(wallet_id, epoch_id, pool_id, "
                                "epochs_delegated, active_stake, base_rewards, adjusted_rewards) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (wallet_id, epoch_id, pools[pool_id]['id'],
                                 epochs_delegated, active_stake, base_rewards, adjusted_rewards))
                else:
                    cur.execute("UPDATE wallets_history SET pool_id = ?, epochs_delegated = ?, "
                                "active_stake = ?, base_rewards = ?, adjusted_rewards = ? "
                                "WHERE wallet_id = ? AND epoch_id = ?",
                                (pools[pool_id]['id'], epochs_delegated,
                                 active_stake, base_rewards, adjusted_rewards, wallet_id, epoch_id))
                conn.commit()

    """
    Generate the excel file from the database
    """
    sql = "SELECT w.stake_address, e.number, wh.epochs_delegated, " \
          "wh.active_stake, wh.adjusted_rewards, p.ticker, w.id, e.id, p.id " \
          "FROM wallets_history wh " \
          "JOIN epochs e ON e.id = wh.epoch_id " \
          "JOIN wallets w ON w.id = wh.wallet_id " \
          "JOIN pools p on p.id = wh.pool_id " \
          "ORDER BY wh.id"
    cur.execute(sql)
    rows = cur.fetchall()
    delegators_per_epoch = {}
    for row in rows:
        stake_address = row[0]
        epoch = row[1]
        epochs_delegated = row[2]
        active_stake = row[3]
        adjusted_rewards = row[4]
        pool = row[5]
        wallet_id = row[6]
        if str(epoch) not in delegators_per_epoch:
            delegators_per_epoch[str(epoch)] = []
        record = {
            'Stake Addr': stake_address,
            'Pool': pool,
            'Epochs delegated': epochs_delegated,
            'Active Stake': active_stake,
            'FACT Rewards': adjusted_rewards
        }
        delegators_per_epoch[str(epoch)].append(record)

    write_excel_file(delegators_per_epoch)

    with open(FILES_PATH + '/delegators_per_epoch.json', 'w') as f:
        f.write(json.dumps(delegators_per_epoch, indent=2))

    """
    Compare the information about the active stake saved into the database
    with the information from the pool_history API
    for all epoch except the current one, which is not provided by the pool_history API
    """
    for epoch in sorted_epochs:
        for pool_id in pools:
            if pool_id in pools_history[epoch]:
                delegators_cnt = pools_history[epoch][pool_id]['delegators_cnt']
                active_stake = pools_history[epoch][pool_id]['active_stake']
                cur.execute("SELECT count(*) AS delegators_cnt, sum(active_stake) FROM wallets_history "
                            "WHERE epoch_id = ? AND pool_id = ?",
                            (epochs[epoch]['id'], pools[pool_id]['id']))
                row = cur.fetchone()
                if delegators_cnt == row[0] and active_stake == row[1]:
                    logging.debug(f"Total active stake for epoch {epoch} pool {pools[pool_id]['ticker']:5s} "
                                  f"matching the data saved in the database")
                    logging.debug(f"Total delegators: {delegators_cnt}, total active stake: {active_stake} lovelace")
                else:
                    logging.error(f"ERROR comparing active stake for "
                                  f"epoch {epoch} pool {pools[pool_id]['ticker']:5s} "
                                  "matching the data saved in the database")
                    logging.error(f"Real data:  total delegators: {delegators_cnt}, "
                                  f"total active stake: {active_stake} lovelace")
                    logging.error(f"Saved data: total delegators: {row[0]}, total active stake: {row[1]} lovelace")
    print(f"Snapshot done in {current_epoch}!")
