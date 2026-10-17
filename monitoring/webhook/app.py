from flask import Flask, request
import logging
import os

app = Flask(__name__)

log_dir = '/var/log/alertmanager'
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'alerts.log')

handler = logging.FileHandler(log_file)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)

@app.route('/alert', methods=['POST'])
def alert():
    payload = request.get_json(force=True)
    app.logger.info('Received alert: %s', payload)
    return '', 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
