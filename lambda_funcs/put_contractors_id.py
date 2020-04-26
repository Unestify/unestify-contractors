import os
import json
import boto3
from typing import Dict, Any

from lambda_layer_get_secrets.get_aws_secrets import get_secret


# Initiate a RDS Data API client
RDSClient = boto3.client('rds-data')

rds_resource_arn = os.environ['DATABASE_ARN']
rds_secret_arn = os.environ['DATABASE_SECRET_ARN']
database_name = os.environ['DATABASE_NAME']
schema = os.environ['SCHEMA']

sql = """
    UPDATE contractors SET
            -- Company information
            company_name = :company_name,
            website = :website,
            service_radius = :service_radius,

            -- Display settings
            display_email = :display_email,
            display_phone = :display_phone,

            -- Qualifications
            years_in_industry = :years_in_industry,
            union_flag = :union_flag,
            workers_comp_flag = :workers_comp_flag,
            licensing_flag = :licensing_flag,
            about_me = :about_me,

            -- Costs
            minimum_service_charge_cents = :minimum_service_charge_cents,
            hourly_rate_cents = :hourly_rate_cents,

            -- Audit trail
            request_id = :request_id,
            modified_by = 0,
            modified_date = current_timestamp 
        
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

    if event["queryStringParameters"] is None:
        raise KeyError("queryStringParameters not included in request")



    # Extract the id from request
    if event["pathParameters"].get("id") is None:
        raise KeyError("contractor id not included in path")
    # Convert string of object_id to integer
    try:
        object_id = event["pathParameters"].get("id")
        object_id = int(object_id)
        contractor_id_param = {"name": "id", "value": {"longValue": object_id}}
    except Exception as e:
        print("Could not convert contractor_id to valid integer id")


    # company_name
    # Required: verify its presence
    if event["queryStringParameters"].get("company_name") is None:
        raise KeyError("company_name not included in queryStringParameters")
    try:
        company_name = event["queryStringParameters"].get('company_name')
        company_name_dict = {"stringValue": company_name}
        company_name_param = {"name": "company_name", "value": company_name_dict}
    except Exception as e:
        print("Could not validate company_name")
        print(e)

    # website
    # Optional:
    if event["queryStringParameters"].get("website") is None:
        website_dict = {"isNull": True}
    else:
        website = event["queryStringParameters"].get('website')
        website_dict = {"stringValue": website}
    website_param = {"name": "website", "value": website_dict}

    # service_radius
    # Required: verify its presence
    if event["queryStringParameters"].get("service_radius") is None:
        raise KeyError("service_radius not included in queryStringParameters")
    try:
        # Required: verify its presence and numerical
        service_radius = event["queryStringParameters"].get('service_radius')
        service_radius_dict = {"longValue": int(service_radius)}
        service_radius_param = {"name": "service_radius", "value": service_radius_dict}
    except Exception as e:
        print("Could not validate service_radius")
        print(e)

    # display_email
    # Optional: Required with default True
    if event["queryStringParameters"].get("display_email") is None:
        display_email_dict = {"booleanValue": True}
    else:
        display_email = event["queryStringParameters"].get('display_email')
        display_email_dict = {"booleanValue": display_email == 1}
    display_email_param = {"name": "display_email", "value": display_email_dict}

    # display_phone
    # Optional: Required with default True
    if event["queryStringParameters"].get("display_phone") is None:
        display_phone_dict = {"booleanValue": True}
    else:
        display_phone = event["queryStringParameters"].get('display_phone')
        display_phone_dict = {"booleanValue": display_phone == 1}
    display_phone_param = {"name": "display_phone", "value": display_phone_dict}

    # years_in_industry
    # Optional: Check integer
    if event["queryStringParameters"].get("years_in_industry") is None:
        years_in_industry_dict = {"isNull": True}
    else:
        years_in_industry = event["queryStringParameters"].get('years_in_industry')
        years_in_industry_dict = {"longValue": int(years_in_industry)}
    years_in_industry_param = {"name": "years_in_industry", "value": years_in_industry_dict}

    # union_flag
    # Optional: Required with default False
    if event["queryStringParameters"].get("union_flag") is None:
        union_flag_dict = {"booleanValue": False}
    else:
        union_flag = event["queryStringParameters"].get('union_flag')
        union_flag_dict = {"booleanValue": union_flag == 1}
    union_flag_param = {"name": "union_flag", "value": union_flag_dict}

    # workers_comp_flag
    # Optional: Required with default False
    if event["queryStringParameters"].get("workers_comp_flag") is None:
        workers_comp_flag_dict = {"booleanValue": False}
    else:
        workers_comp_flag = event["queryStringParameters"].get('workers_comp_flag')
        workers_comp_flag_dict = {"booleanValue": workers_comp_flag == 1}
    workers_comp_flag_param = {"name": "workers_comp_flag", "value": workers_comp_flag_dict}

    # licensing_flag
    # Optional: Required with default False
    if event["queryStringParameters"].get("licensing_flag") is None:
        licensing_flag_dict = {"booleanValue": False}
    else:
        licensing_flag = event["queryStringParameters"].get("licensing_flag")
        licensing_flag_dict = {"booleanValue": licensing_flag == 1}
    licensing_flag_param = {"name": "licensing_flag", "value": licensing_flag_dict}

    # about_me
    # Optional:
    if event["queryStringParameters"].get("about_me") is None:
        about_me_dict = {"isNull": True}
    else:
        about_me = event["queryStringParameters"].get("about_me")
        about_me_dict = {"stringValue": about_me}
    about_me_param = {"name": "about_me", "value": about_me_dict}

    # minimum_service_charge_cents
    # Optional: Required with default 0
    if event["queryStringParameters"].get("minimum_service_charge_cents") is None:
        minimum_service_charge_cents_dict = {"longValue": int(0)}
    else:
        minimum_service_charge_cents = event["queryStringParameters"].get("minimum_service_charge_cents")
        minimum_service_charge_cents_dict = {"longValue": int(minimum_service_charge_cents)}
    minimum_service_charge_cents_param = {"name": "minimum_service_charge_cents",
                                          "value": minimum_service_charge_cents_dict}

    # hourly_rate_cents
    # Optional:
    if event["queryStringParameters"].get("hourly_rate_cents") is None:
        hourly_rate_cents_dict = {"isNull": True}
    else:
        hourly_rate_cents = event["queryStringParameters"].get("hourly_rate_cents")
        hourly_rate_cents_dict = {"longValue": int(hourly_rate_cents)}
    hourly_rate_cents_param = {"name": "hourly_rate_cents", "value": hourly_rate_cents_dict}

    # request_id
    # Required metadata: verify not redundant call to db
    request_id = context.aws_request_id
    request_id_param = {"name": "request_id", "value": {"stringValue": request_id}}

    parameters = [
        contractor_id_param,
        company_name_param,
        website_param,
        service_radius_param,
        display_email_param,
        display_phone_param,
        years_in_industry_param,
        union_flag_param,
        workers_comp_flag_param,
        licensing_flag_param,
        about_me_param,
        minimum_service_charge_cents_param,
        hourly_rate_cents_param,
        request_id_param
    ]

    try:
        print("attempting to execute statement")
        response = RDSClient.execute_statement(
            resourceArn=rds_resource_arn,
            secretArn=rds_secret_arn,
            database=database_name,
            sql=sql,
            parameters=parameters,
            includeResultMetadata=True
        )

    except Exception as e:
        print("Could not complete insert statement. Rolling back transaction.")
        print(e)

    # serialization = serialize_response('contractors', response)

    # body = {'data': serialization}

    return {
        'statusCode': 200,
        'body': json.dumps(response),
        'headers': {'Content-Type': 'application/json'}
    }