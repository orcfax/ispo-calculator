from config import *
from flask import Flask, request, make_response
from flask_restx import Api, Resource
from werkzeug.middleware.proxy_fix import ProxyFix
import sqlite3
import logging.handlers
import datetime

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
logging.basicConfig(filename=API_LOG_FILE, format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.DEBUG)

"""
Create the Flask application
"""
app = Flask(__name__)
app.config['DEBUG'] = True
app.config['UPLOAD_FOLDER'] = FILES_PATH
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
            logging.warning(f"'/get_rewards/{stake_address}: invalid stake address")
            return msg, 406
        try:
            conn = sqlite3.connect(DB_NAME)
            cur = conn.cursor()
            cur.execute("SELECT sum(rewards_amount) FROM wallets_history wh "
                        "JOIN wallets w ON wh.wallet_id = w.id WHERE w.stake_address = ?", (stake_address,))
            row = cur.fetchone()
        except Exception as err:
            logging.warning(f"'/get_rewards/{stake_address}")
            logging.exception(err)
            msg = {
                "error": f"Server error: {e}",
                "CODE": "SERVER_ERROR"
            }
            return msg, 503
        else:
            if row[0]:
                rewards = {
                    "stake_address": stake_address,
                    "rewards_amount": str(row[0] / pow(10, DECIMALS))
                }
                resp = make_response(rewards)
                resp.headers['Content-Type'] = 'application/json'
                logging.info(f"'/get_rewards/{stake_address}: {row[0]}")
                return resp
            else:
                logging.warning(f"'/get_rewards/{stake_address}: not found")
                msg = {
                    "error": f"Stake address {stake_address} not found!"
                }
                return msg


if __name__ == '__main__':
    logging.info('Starting')
    app.run(
        threaded=True,
        host='0.0.0.0',
        port=API_PORT
    )
