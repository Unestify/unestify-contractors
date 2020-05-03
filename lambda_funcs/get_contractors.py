
"""
An API for accessing contractor

"""

import os
import json
import boto3
from typing import Dict, Any

# From Layers
from lambda_layer_sqlutilities.parse_utilities import serialize_response
from lambda_layer_get_secrets.get_aws_secrets import get_secret
import googlemaps


# Initiate a RDS Data API client
RDSClient = boto3.client('rds-data')

rds_resource_arn = os.environ['DATABASE_ARN']
rds_secret_arn = os.environ['DATABASE_SECRET_ARN']
database_name = os.environ['DATABASE_NAME']
gmap_secret_arn = os.environ['MAPS_API_KEY']

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
        
        -- When 0 value_ratings have been submitted, return rating of 0, not TRUE
        CASE 
            WHEN count(contractor_ratings.value_rating) = 0 THEN 0
            ELSE avg(contractor_ratings.value_rating)
        END                                         AS value_rating,
        count(contractor_ratings.value_rating)		AS value_rating_count,
        
        -- When 0 on_budget_ratings have been submitted, return rating of 0, not TRUE
        CASE 
            WHEN count(contractor_ratings.on_budget_rating) = 0 THEN 0
            ELSE avg(contractor_ratings.on_budget_rating)
        END                                         AS on_budget_rating,
        count(contractor_ratings.on_budget_rating)	AS on_budget_rating_count,
        
        -- When 0 on_time_ratings have been submitted, return rating of 0, not TRUE
        CASE 
            WHEN count(contractor_ratings.on_time_rating) = 0 THEN 0
            ELSE avg(contractor_ratings.on_time_rating)
        END                                         AS on_time_rating,
        count(contractor_ratings.on_time_rating)	AS on_time_rating_count,
        json_agg(DISTINCT(trades.name))             AS trades_string,
        
        ST_DISTANCE_SPHERE(
           latlon,
           ST_SetSRID(
               ST_MakePoint(:lng, :lat), 4326)
        )/1609                                      AS distance
        
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
        contractors.soft_delete IS false AND 
        ST_DISTANCE_SPHERE(
           contractors.latlon,
           ST_SetSRID(
               ST_MakePoint(:lng, :lat), 4326)
        ) <= contractors.service_radius*1609
     
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

    if 'q' not in event['queryStringParameters']:
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
            'body': 'Request must include location query string parameter "q"',
        }

    locale = event['queryStringParameters'].get('q')

    secret = get_secret(gmap_secret_arn, 'us-east-2')


    api_key = json.loads(secret).get('gmaps_api_key')

    gmaps = googlemaps.client.Client(key=api_key)

    # Geocoding an address

    geocode_result = gmaps.geocode(locale)

    latlon_dict = geocode_result[0].get('geometry').get('location')
    lat = latlon_dict.get('lat')
    lat_dict = {"doubleValue": float(lat)}
    lat_param = {"name": "lat", "value": lat_dict}

    lng = latlon_dict.get('lng')
    lng_dict = {"doubleValue": float(lng)}
    lng_param = {"name": "lng", "value": lng_dict}

    parameters = [lat_param, lng_param]


    response = RDSClient.execute_statement(
        resourceArn=rds_resource_arn,
        secretArn=rds_secret_arn,
        database=database_name,
        sql=sql,
        parameters=parameters,
        includeResultMetadata=True
    )

    serialization = serialize_response('contractors', response)

    # # Array values cannot be passed to database via Data API, so trades are filtered outside of database
    # if event['multiValueQueryStringParameters'] and event['multiValueQueryStringParameters'].get('trades'):
    #     # Extract list of trades from query string parameters
    #     trade_filter_list = event['multiValueQueryStringParameters'].get('trades')
    #
    #     contractors_filtered = []
    #     # Iterate over all contractors returned by query: check to make sure all trades are in contractor record
    #     for contractor in serialization:
    #         # If contractor "trades_string" object includes all trades passed in query parameters, add it to output
    #         if all([trade in contractor.get('trades_string') for trade in trade_filter_list]):
    #             contractors_filtered.append(contractor)
    # # If multiValueQueryStringParameters does not include
    # else:
    #     contractors_filtered = serialization

    # Return filtered list of contractors
    body = {'data': serialization}

    return {
                'statusCode': 200,
                'body': json.dumps(body),
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
    }
