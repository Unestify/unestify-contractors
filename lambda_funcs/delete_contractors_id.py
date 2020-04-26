import os
import json
import boto3
from typing import Dict, Any

from sqlutilities.parse_utilities import serialize_response


# Initiate a RDS Data API client
RDSClient = boto3.client('rds-data')

rds_resource_arn = os.environ['DATABASE_ARN']
rds_secret_arn = os.environ['DATABASE_SECRET_ARN']
database_name = os.environ['DATABASE_NAME']


sql = """
    UPDATE contractors SET 
        soft_delete = true
    FROM 
        contractors
    WHERE
        id = :id
    ;
"""


def handler(event, context) -> Dict[str, Any]:
    """

    :param event:
    :param context:
    :return:
    """

    # Extract the id from request
    object_id = event["pathParameters"].get("id")

    # Convert string of object_id to integer
    try:
        object_id = int(object_id)

    # If integer conversion fails, raise ValueError
    except ValueError:
        return {
                    'statusCode': 400,
                    'body': 'Invalid input, please input an integer',
                    'headers': {'Content-Type': 'application/json'}
                }

    # Convert id into parameter specific format: {"name": variable_name, "value": {dataType: value}}
    param1 = {"name": "id", "value": {"longValue": object_id}}
    parameters = [param1]

    response = RDSClient.execute_statement(
        resourceArn=rds_resource_arn,
        secretArn=rds_secret_arn,
        database=database_name,
        sql=sql,
        parameters=parameters,
        includeResultMetadata=True
    )

    serialization = serialize_response('contractors', response)

    body = {'data': serialization}

    return {
                'statusCode': 200,
                'body': json.dumps(body),
                'headers': {'Content-Type': 'application/json'}
            }