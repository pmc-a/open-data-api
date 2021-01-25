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

    response = table.scan(Limit=limit)

    # Parses the boto3 Decimal objects into native Py integers
    parsed_items = replace_decimals(response["Items"])

    return jsonify(parsed_items)


@app.route("/crime/<table_name>/year/<year>", methods=['GET'])
def get_crime_data_by_year(table_name, year):
    """
    Crime data
    ---
    get:
      description: Get crime data by year
      responses:
        200
    """
    table = dynamo_client.Table(table_name)

    if year is None:
      return Response(status=400)
    else:
      response = table.scan(
          FilterExpression="Calendar_Year = :Calendar_Year",
          ExpressionAttributeValues={
          ":Calendar_Year": int(year)
          }
      )
      
      # Parses the boto3 Decimal objects into native Py integers
      parsed_items = replace_decimals(response["Items"])

      return jsonify(parsed_items)


@app.route("/environment/<table_name>", methods=['GET'])
def get_env_data(table_name):
    """
    Environment data
    ---
    get:
      description: Get environment data about Belfast trees
      responses:
        200
    """
    limit = 10 if request.args.get('limit') is None else int(request.args.get('limit'))
    table = dynamo_client.Table(table_name)
    response = table.scan(Limit=limit)

    # Parses the boto3 Decimal objects into native Py integers
    parsed_items = replace_decimals(response["Items"])

    return jsonify(parsed_items)


@app.route("/environment/<table_name>/tree/<species>", methods=['GET'])
def get_env_data_by_tree(table_name, species):
    """
    Environment data
    ---
    get:
      description: Get environment data about Belfast trees by tree type
      responses:
        200
    """
    table = dynamo_client.Table(table_name)

    if species is None:
      return Response(status=400)
    else:
      response = table.scan(
          FilterExpression="SPECIESTYPE = :SPECIESTYPE",
          ExpressionAttributeValues={
          ":SPECIESTYPE": species
          }
      )
      
      # Parses the boto3 Decimal objects into native Py integers
      parsed_items = replace_decimals(response["Items"])

      return jsonify(parsed_items)

