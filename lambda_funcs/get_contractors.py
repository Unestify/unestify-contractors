
import os
import json
import boto3
from typing import Dict, Any

from sqlutilities.parse_utilities import serialize_response


# Initiate a RDS Data API client
rds_client = boto3.client('rds-data')

resourceArn = os.environ['DATABASE_ARN']
secretArn = os.environ['DATABASE_SECRET_ARN']
database = os.environ['DATABASE_NAME']

# This will eventually support all of our contractor search filters!
sql = """
    SELECT 
        users.first_name							AS first_name,
        users.last_name								AS last_name,
        CASE
            WHEN contractors.display_email THEN users.email
            ELSE NULL
        END 										AS email,
        CASE
            WHEN contractors.display_phone THEN users.phone
            ELSE NULL
        END 										AS phone,
        users.city 									AS city,
    
        contractors.id								AS contractor_id,
        contractors.company_name 					AS company_name,
        contractors.website 						AS website,
        contractors.years_in_industry 				AS years_in_industry,
        contractors.about_me						AS about_me,
        contractors.verified_contractor				AS verified_contractor,
        contractors.slug							AS slug,
        
        contractors.profile_picture_s3				AS profile_picture_s3,
        contractors.portfolio_picture_s3			AS portfolio_picture_s3,
        
        contractors.minimum_service_charge_cents    AS minimum_service_charge_cents,
        contractors.hourly_rate_cents               AS hourly_rate_cents,
        
        contractors.union_flag                      AS union_flag,
        contractors.workers_comp_flag               AS workers_comp_flag,
        contractors.licensing_flag                  AS licensing_flag,
        
        avg(contractor_ratings.value_rating)		AS value_rating,
        count(contractor_ratings.value_rating)		AS value_rating_count,
        avg(contractor_ratings.on_budget_rating)	AS on_budget_rating,
        count(contractor_ratings.on_budget_rating)	AS on_budget_rating_count,
        avg(contractor_ratings.on_time_rating)		AS on_time_rating,
        count(contractor_ratings.on_time_rating)	AS on_time_rating_count,
        
        string_agg(DISTINCT(trades.name), ', ')     AS trades_string
        
    FROM 
        contractors
    
    LEFT JOIN 
        users
    ON
        users.id = contractors.user_id
    
    LEFT JOIN
        contractor_ratings
    ON
        contractor_ratings.contractor_id = contractors.id
        
    LEFT JOIN 
        contractor_unit_prices 
    ON
        contractors.id = contractor_unit_prices.contractor_id
    
    LEFT JOIN
        subtrades
    ON
        subtrades.id = contractor_unit_prices.subtrade_id
    
    LEFT JOIN
        trades
    ON
        trades.id = subtrades.trade_id
        
    WHERE 
        contractors.soft_delete IS false
     
    GROUP BY
        users.id,
        contractors.id
    ;
"""


def handler(event, context) -> Dict[str, Any]:
    """

    :param event:
    :param context:
    :return:
    """

    response = rds_client.execute_statement(
        resourceArn=resourceArn,
        secretArn=secretArn,
        database=database,
        sql=sql,
        includeResultMetadata=True
    )

    serialization = serialize_response('contractors', response)

    # Array values cannot be passed to database via Data API, so trades are filtered outside of database
    if event['multiValueQueryStringParameters'] and event['multiValueQueryStringParameters'].get('trades'):
        # Extract list of trades from query string parameters
        trade_filter_list = event['multiValueQueryStringParameters'].get('trades')

        contractors_filtered = []
        # Iterate over all contractors returned by query: check to make sure all trades are in contractor record
        for contractor in serialization:
            # If contractor "trades_string" object includes all trades passed in query parameters, add it to output
            if all([trade in contractor.get('trades_string') for trade in trade_filter_list]):
                contractors_filtered.append(contractor)
    # If multiValueQueryStringParameters does not include
    else:
        contractors_filtered = serialization

    # Return filtered list of contractors
    body = {'data': contractors_filtered}

    return {
                'statusCode': 200,
                'body': json.dumps(body),
                'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            }
