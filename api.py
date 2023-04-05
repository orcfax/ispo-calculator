from config import *
from flask import Flask, make_response
from flask_restx import Api, Resource
from werkzeug.middleware.proxy_fix import ProxyFix
from koios_api.address import get_address_info
import sqlite3
import logging.handlers

"""
Create some required folders to store log and transaction file
"""
try:
    if not os.path.exists(FILES_PATH):
        os.mkdir(FILES_PATH)
    if not os.path.exists(DB_PATH):
        os.mkdir(DB_PATH)
except Exception as e:
    print('Error creating the required folders: %s' % e)
    exit(1)

"""
Set up logging
"""
handler = logging.handlers.WatchedFileHandler(API_LOG_FILE)
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
handler.setFormatter(formatter)

applog = logging.getLogger('api')
applog.addHandler(handler)
applog.setLevel(logging.DEBUG)

"""
Create the Flask application
"""
app = Flask(__name__)
app.config['DEBUG'] = True
app.wsgi_app = ProxyFix(app.wsgi_app)

api = Api(app, version=API_VERSION_MINOR, title=API_NAME, description=API_DESCRIPTION,)
ns = api.namespace('api/' + API_VERSION_MAJOR, description=API_NAME + ' ' + API_VERSION_MINOR)


@ns.route('/')
class Home(Resource):
    @staticmethod
    def get():
        return '<h1>' + API_NAME + ' ' + API_VERSION_MINOR + '</h1>'


@ns.route('/get_rewards/<string:stake_address>')
@api.response(200, "OK")
@api.response(403, "Not Acceptable client error")
@api.response(503, "Server error")
class EventGetRewards(Resource):
    """
    Get the rewards accumulated for this stake address
    If a payment address is given, the Koios API is used to find out the stake address
    """
    @staticmethod
    def get(stake_address):
        if len(stake_address) == 103 and stake_address.startswith('addr1'):
            """
            Payment address instead of stake address.
            Find out the stake address. First try to find it out from the database.
            If not saved into the database, find it out from the Koios API and save it into the database.
            """
            payment_address = stake_address
            try:
                conn = sqlite3.connect(DB_NAME)
                cur = conn.cursor()
                sql = "SELECT w.stake_address FROM wallets w " \
                      "JOIN wallets_addresses wa ON w.id = wa.wallet_id " \
                      "WHERE wa.payment_address = ?"
                cur.execute(sql, (payment_address,))
                row = cur.fetchone()
                if row:
                    stake_address = row[0]
                else:
                    addr_info = get_address_info(stake_address)
                    stake_address = addr_info[0]['stake_address']
                    sql = "SELECT id FROM wallets WHERE stake_address = ?"
                    cur.execute(sql, (stake_address,))
                    row = cur.fetchone()
                    if row:
                        wallet_id = row[0]
                        sql = "INSERT INTO wallets_addresses(wallet_id, payment_address) VALUES (?, ?)"
                        cur.execute(sql, (wallet_id, payment_address))
                        conn.commit()
                    else:
                        applog.warning(f"/get_rewards/{stake_address}: not found")
                        msg = {
                            "error": f"Stake of payment address {stake_address} not found!"
                        }
                        return msg
                conn.close()
            except Exception as err:
                applog.error(f"/get_rewards/{stake_address}")
                applog.exception(err)
                msg = {
                    "error": f"Server error: {err}",
                    "CODE": "SERVER_ERROR"
                }
                return msg, 503
        if len(stake_address) != 59 or not stake_address.startswith('stake1'):
            msg = {
                'error': 'Invalid stake address or payment address!'
            }
            applog.warning(f"'/get_rewards/{stake_address}: invalid stake address or payment address")
            return msg, 406
        try:
            conn = sqlite3.connect(DB_NAME)
            cur = conn.cursor()
            sql = "SELECT max(e.number) FROM epochs e JOIN pools_epochs pe ON e.id = pe.epoch_id"
            cur.execute(sql)
            row_epoch = cur.fetchone()
            sql = "SELECT sum(base_rewards), sum(adjusted_rewards) FROM wallets_history"
            cur.execute(sql)
            row_rewards = cur.fetchone()
            sql = "SELECT ps.active_stake, ps.live_stake " \
                  "FROM pools_stake ps JOIN pools p ON p.id = ps.pool_id " \
                  "WHERE p.pool_id_bech32 = ?"
            cur.execute(sql, (POOL_IDS_BECH32[0],))
            row_stake = cur.fetchone()
            sql = "SELECT e.number, wh.active_stake, wh.base_rewards, wh.adjusted_rewards " \
                  "FROM wallets_history wh JOIN epochs e on e.id = wh.epoch_id " \
                  "JOIN wallets w ON wh.wallet_id = w.id WHERE w.stake_address = ?"
            cur.execute(sql, (stake_address,))
            rows = cur.fetchall()
        except Exception as err:
            applog.error(f"/get_rewards/{stake_address}")
            applog.exception(err)
            msg = {
                "error": f"Server error: {err}",
                "CODE": "SERVER_ERROR"
            }
            return msg, 503
        else:
            latest_epoch = row_epoch[0]
            ispo_base_rewards = row_rewards[0] / pow(10, DECIMALS)
            ispo_adjusted_rewards = row_rewards[1] / pow(10, DECIMALS)
            active_stake = row_stake[0]
            live_stake = row_stake[1]
            rewards = []
            total_base_rewards = 0
            total_adjusted_rewards = 0
            for row in rows:
                epoch = row[0]
                active_stake = row[1]
                base_rewards = row[2] / pow(10, DECIMALS)
                adjusted_rewards = row[3] / pow(10, DECIMALS)
                total_base_rewards += row[2] / pow(10, DECIMALS)
                total_adjusted_rewards += row[3] / pow(10, DECIMALS)
                rewards.append(
                    {
                        'epoch': str(epoch),
                        'active_stake': str(active_stake),
                        'base_rewards': str(base_rewards),
                        'bonus': str(adjusted_rewards - base_rewards),
                        'adjusted_rewards': str(adjusted_rewards)
                    }
                )
            if total_base_rewards > 0:
                resp = make_response(
                    {
                        'latest_epoch': str(latest_epoch),
                        'stake_address': stake_address,
                        'active_stake': active_stake,
                        'live_stake': live_stake,
                        'rewards': rewards,
                        'bonus': str(total_adjusted_rewards - total_base_rewards),
                        'total_base_rewards': str(total_base_rewards),
                        'total_bonus': str(total_adjusted_rewards - total_base_rewards),
                        'total_adjusted_rewards': str(total_adjusted_rewards),
                        'ispo_total_base_rewards': str(ispo_base_rewards),
                        'ispo_total_bonus': str(ispo_adjusted_rewards - ispo_base_rewards),
                        'ispo_total_adjusted_rewards': str(ispo_adjusted_rewards),
                        'rewards_percentage_from_total': str(100 * total_adjusted_rewards / ispo_adjusted_rewards) + ' %'
                    }
                )
                resp.headers['Content-Type'] = 'application/json'
                applog.info(f"/get_rewards/{stake_address}: total base rewards =  {total_base_rewards}, "
                            f"total adjusted rewards =  {total_adjusted_rewards}")
                return resp
            else:
                applog.warning(f"/get_rewards/{stake_address}: not found")
                msg = {
                    "error": f"Stake of payment address {stake_address} not found!"
                }
                return msg


@ns.route('/get_total_rewards/')
@api.response(200, "OK")
@api.response(503, "Server error")
class EventGetTotalRewards(Resource):
    """
    Get the rewards accumulated for the whole ISPO
    """
    @staticmethod
    def get():
        try:
            conn = sqlite3.connect(DB_NAME)
            cur = conn.cursor()
            sql = "SELECT sum(base_rewards), sum(adjusted_rewards) FROM wallets_history"
            cur.execute(sql)
            row_rewards = cur.fetchone()
            sql = "SELECT ps.active_stake, ps.live_stake " \
                  "FROM pools_stake ps JOIN pools p ON p.id = ps.pool_id " \
                  "WHERE p.pool_id_bech32 = ?"
            cur.execute(sql, (POOL_IDS_BECH32[0],))
            row_stake = cur.fetchone()
            sql = "SELECT max(e.number) FROM epochs e JOIN pools_epochs pe ON e.id = pe.epoch_id"
            cur.execute(sql)
            row_epoch = cur.fetchone()
        except Exception as err:
            applog.warning('/get_total_rewards/')
            applog.exception(err)
            msg = {
                "error": f"Server error: {err}",
                "CODE": "SERVER_ERROR"
            }
            return msg, 503
        else:
            base_rewards = str(row_rewards[0] / pow(10, DECIMALS))
            adjusted_rewards = str(row_rewards[1] / pow(10, DECIMALS))
            bonus = str((row_rewards[1] - row_rewards[0]) / pow(10, DECIMALS))
            active_stake = row_stake[0]
            live_stake = row_stake[1]
            epoch = row_epoch[0]
            resp = make_response(
                {
                    'latest_epoch': epoch,
                    'base_rewards': base_rewards,
                    'bonus': bonus,
                    'adjusted_rewards': adjusted_rewards,
                    'active_stake': active_stake,
                    'live_stake': live_stake
                }
            )
            resp.headers['Content-Type'] = 'application/json'
            applog.info(f"/get_total_rewards: base rewards: {base_rewards}, adjusted rewards: {adjusted_rewards}")
            return resp


if __name__ == '__main__':
    applog.info('Starting')
    app.run(
        threaded=True,
        host='0.0.0.0',
        port=API_PORT
    )
