from flask import Flask, Response, jsonify
import boto3

from src.utils import replace_decimals


app = Flask(__name__)
dynamo_client = boto3.resource('dynamodb')


@app.route('/health', methods=['GET'])
def health_check():
    return Response(status=200)


@app.route("/crime/<table_name>", methods=['GET'])
def get_crime_data(table_name):
    table = dynamo_client.Table(table_name)

    response = table.scan()

    # Parses the boto3 Decimal objects into native Py integers
    parsed_items = replace_decimals(response["Items"])

    return jsonify(parsed_items)
