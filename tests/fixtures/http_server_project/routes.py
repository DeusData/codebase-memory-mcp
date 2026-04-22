from flask import Flask

app = Flask(__name__)


@app.route('/v1/score', methods=['POST'])
def score():
    return {}


@app.route('/v1/orders', methods=['POST'])
def orders():
    return {}
