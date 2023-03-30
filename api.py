from config import *
from flask import Flask, make_response
from flask_restx import Api, Resource
from werkzeug.middleware.proxy_fix import ProxyFix
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
# logging.basicConfig(filename=API_LOG_FILE, format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)
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
    """
    def get(self, stake_address):
        if len(stake_address) != 59 or not stake_address.startswith('stake1'):
            msg = {
                'error': 'Invalid stake address!'
            }
            applog.warning(f"'/get_rewards/{stake_address}: invalid stake address")
            return msg, 406
        try:
            conn = sqlite3.connect(DB_NAME)
            cur = conn.cursor()
            sql = "SELECT e.number, wh.active_stake, wh.adjusted_rewards " \
                  "FROM wallets_history wh JOIN epochs e on e.id = wh.epoch_id " \
                  "JOIN wallets w ON wh.wallet_id = w.id WHERE w.stake_address = ?"
            cur.execute(sql, (stake_address,))
            rows = cur.fetchall()
        except Exception as err:
            applog.warning(f"/get_rewards/{stake_address}")
            applog.exception(err)
            msg = {
                "error": f"Server error: {e}",
                "CODE": "SERVER_ERROR"
            }
            return msg, 503
        else:
            rewards = []
            total_rewards = 0
            for row in rows:
                epoch = str(row[0])
                active_stake = str(row[1])
                total_rewards += row[2] / pow(10, DECIMALS)
                rewards_amount = str(row[2] / pow(10, DECIMALS))
                rewards.append(
                    {
                        'epoch': epoch,
                        'active_stake': active_stake,
                        'rewards': rewards_amount
                    }
                )
            if total_rewards > 0:
                resp = make_response(
                    {
                        'stake_address': stake_address,
                        'rewards': rewards,
                        'total_rewards': str(total_rewards)
                    }
                )
                resp.headers['Content-Type'] = 'application/json'
                applog.info(f"/get_rewards/{stake_address}: total rewards =  {total_rewards}")
                return resp
            else:
                applog.warning(f"/get_rewards/{stake_address}: not found")
                msg = {
                    "error": f"Stake address {stake_address} not found!"
                }
                return msg


if __name__ == '__main__':
    applog.info('Starting')
    app.run(
        threaded=True,
        host='0.0.0.0',
        port=API_PORT
    )
