from flask import Flask,jsonify,Response,request

app = Flask(__name__)

@app.route('/init', methods=['GET'])
async def init():
    print("In Init")
    return jsonify({'message': 'Response OK!','status': 'COMPLETED'})
@app.route('/test', methods=['GET'])
async def test_get():
    print("In test get")
    return jsonify({'message': 'Response OK!','job_status': 'COMPLETED'})
@app.route('/test', methods=['POST'])
async def test_post():
    print("In test post")
    headers = request.headers
    print("headers: ",headers)
    json = request.get_json()
    print("json: ",json)
    return jsonify({'message': 'Response OK!','job_status': 'COMPLETED'})
if __name__ == '__main__':
 #   app.run(ssl_context='adhoc', host='0.0.0.0', port=5443, debug=True)
    app.run( host='0.0.0.0', port=5443, debug=True)
