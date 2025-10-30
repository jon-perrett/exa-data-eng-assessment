from flask import Flask, request, make_response
from adaptor.minio import MinIOClient
import os
import logging

app = Flask(__name__)
minio_client = MinIOClient(os.environ["MINIO_ENDPOINT"], os.environ["MINIO_USER"], os.environ["MINIO_PWD"])

@app.route("/health", methods=["GET"])
def healthcheck():
    return make_response("", 200)

@app.route('/transform', methods=['POST'])
def get_webhook():
    # obtain the request event from the 'POST' call
    event = request.json

    for event in event["Records"]:
        bucket = event["s3"]["bucket"]["name"]
        key = event["s3"]["object"]["key"]
        body = minio_client.get_object(bucket, key)
        logging.warn("s3://%s/%s is size %d", bucket, key, len(body))
    resp = make_response("", 200)
    # resp.headers['x-amz-request-route'] = request_route
    # resp.headers['x-amz-request-token'] = request_token
    return resp

if __name__ == '__main__':
    app.run()