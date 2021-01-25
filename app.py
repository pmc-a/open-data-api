from flask import Flask, Response, jsonify, request
from boto3.dynamodb.conditions import Key
import boto3

from src.utils import replace_decimals

app = Flask(__name__)
dynamo_client = boto3.resource('dynamodb')


@app.route('/health', methods=['GET'])
def health_check():
    """
    Health Check
    ---
    get:
      description: Health check to verify service is running
      responses:
        200:
            description: The resource was deleted successfully.
    """
    return Response(status=200)


@app.route("/crime/<table_name>", methods=['GET'])
def get_crime_data(table_name):
    """
    Crime data
    ---
    get:
      description: Get crime data
      responses:
        200
    """
    limit = 10 if request.args.get('limit') is None else int(request.args.get('limit'))
    table = dynamo_client.Table(table_name)

    filter_value_present = False if request.args.get('year') is None else True

    if filter_value_present:
      response = table.scan(
          FilterExpression="Calendar_Year = :Calendar_Year",
          ExpressionAttributeValues={
          ":Calendar_Year": int(request.args.get('year'))
          }
      )
    else:
      response = table.scan(Limit=limit)

    # Parses the boto3 Decimal objects into native Py integers
    parsed_items = replace_decimals(response["Items"])

    return jsonify(parsed_items)
