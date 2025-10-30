from flask import Flask, request, abort, make_response
import requests
import MinIO

app = Flask(__name__)
@app.route("/health", methods=["GET"])
def healthcheck():
    return make_response("", 200)

@app.route('/transform', methods=['POST'])
def get_webhook():
    if request.method == 'POST':
        # obtain the request event from the 'POST' call
        event = request.json

        object_context = event["getObjectContext"]

        # Get the presigned URL to fetch the requested
        # original object from MinIO
        s3_url = object_context["inputS3Url"]

        # Extract the route and request token from the input context
        request_route = object_context["outputRoute"]
        request_token = object_context["outputToken"]

        # Get the original S3 object using the presigned URL
        r = requests.get(s3_url)
        original_object = r.content.decode('utf-8')
        print(original_object)

        # Transform all text in the original object to uppercase
        # You can replace it with your custom code based on your use case
        transformed_object = original_object.upper()

        # Write object back to S3 Object Lambda
        # response sends the transformed data
        # back to MinIO and then to the user
        resp = make_response(transformed_object, 200)
        resp.headers['x-amz-request-route'] = request_route
        resp.headers['x-amz-request-token'] = request_token
        return resp

    else:
        abort(400)

if __name__ == '__main__':
    app.run()