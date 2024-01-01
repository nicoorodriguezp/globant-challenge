from flask import Flask, jsonify

app = Flask(__name__)

@app.route('/healthcheck/', methods=['GET'])
def healthcheck():
    return jsonify({'status': 'OK', 'message': 'The API is up and running.'}), 200

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
