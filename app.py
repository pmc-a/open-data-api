from flask import Flask
import boto3


app = Flask(__name__)
s3 = boto3.resource('s3')


@app.route("/")
def hello():
    for bucket in s3.buckets.all():
        print(bucket.name)

    return "Hello World!"
