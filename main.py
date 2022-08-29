import json
import requests
import time
import datetime
import io
import psycopg2
import os 
import base64
import functions_framework

# ENV and CONFIG
# SECRETS
# these three need to be secrets, defined in this project
postgres_server           = os.environ.get('postgres_server', 'postgres_server environment variable is not set.')
postgres_password         = os.environ.get('postgres_password', 'postgres_password environment variable is not set.') 
satori_serviceaccount_key = os.environ.get('satori_serviceaccount_key', 'satori_serviceaccount_key environment variable is not set.')

# ENV VARS
# the rest of these need to be runtime env vars defined in the cloud function
postgres_port             = os.environ.get('postgres_port', 'postgres_port environment variable is not set.')
postgres_username         = os.environ.get('postgres_username', 'postgres_username environment variable is not set.')
postgres_database_name    = os.environ.get('postgres_database_name', 'postgres_database_name environment variable is not set.')
# schema and table, we don't create a schema in this example
# but we do drop-if-not-exist and then create a new table
postgres_schema_name      = os.environ.get('postgres_schema_name', 'postgres_schema environment variable is not set.')
postgres_table_name       = os.environ.get('postgres_table_name', 'postgres_table_name environment variable is not set.')
# Satori Authentication
# see https://app.satoricyber.com/docs/api for auth config info
satori_serviceaccount_id  = os.environ.get('satori_serviceaccount_id', 'satori_serviceaccount_id environment variable is not set.')
satori_account_id         = os.environ.get('satori_account_id', 'satori_account_id environment variable is not set.')
satori_host               = os.environ.get('satori_api_host', 'satori_api_host environment variable is not set.')

# since this solution is gcp-specific, we are using a unix socket to the cloud sql
# instance name, and then running this cloud function as an accessor to that resource
unix_socket = '/cloudsql/{}'.format(postgres_server)


# Retriever Function, Satori Audit Data via Rest API
# we define a function to retrieve audit entries / data flows, 
# this is called in the final jupyter block below
# This is a sample only, do not use in production!
def getAuditLogs(audit_days_ago_to_yesterday):

    # This function retrieves Satori audit entries from the last thirty days up to yesterday
    yesterday_start = datetime.date.today() - datetime.timedelta(audit_days_ago_to_yesterday)
    unix_time_start = yesterday_start.strftime("%s") + "000"
    unix_time_end = str(int(yesterday_start.strftime("%s")) + (86400*30)) + "000"

    # Authenticate to Satori for a bearer token
    authheaders = {'content-type': 'application/json','accept': 'application/json'}
    url = "https://{}/api/authentication/token".format(satori_host)
    try:
        r = requests.post(url, 
                          headers=authheaders, 
                          data='{"serviceAccountId": "' + satori_serviceaccount_id + 
                          '", "serviceAccountKey": "' + satori_serviceaccount_key + '"}')
        response = r.json()
        satori_token = response["token"]
    except Exception as err:
        print("Bearer Token Failure: :", err)
        print("Exception TYPE:", type(err))
    else:
        # psycopg2 expects a file-like object
        sqlFile = io.StringIO()
    
    # build request to rest API for audit entries, aka "data flows"
    payload = {}
    headers = {
    'Authorization': 'Bearer {}'.format(satori_token),
    }
    auditurl = "https://{}/api/data-flow/{}/export?from={}&to={}".format(satori_host,
                                                                         satori_account_id,
                                                                         unix_time_start,
                                                                         unix_time_end)
    try:
        response = requests.get(auditurl, headers=headers, data=payload)
        response.raise_for_status()
    except requests.exceptions.RequestException as err:
        print("Retrieval of audit data failed: :", err)
        print("Exception TYPE:", type(err))
    else:
        # return a StringIO object which is what psycopg2 wants to see for its bulk load call
        return io.StringIO(response.text)

# MAIN WORK: create a temp table, copy API data into it, 
# then insert from temp table to final table
# We assume your connection user has the privileges for this

@functions_framework.cloud_event
def mainwork(cloud_event):

    audit_days_ago_to_yesterday = int(base64.b64decode(cloud_event.data["message"]["data"]).decode())

    print("Requesting Satori audit info, days ago to yesterday: " + str(audit_days_ago_to_yesterday))

    conn = psycopg2.connect(database=postgres_database_name,
                            user=postgres_username, 
                            password=postgres_password, 
                            host=unix_socket, 
                            port=postgres_port)

    conn.autocommit = True
    cursor = conn.cursor()


    ###############################################################
    # STEP 1: create our final destination table for audit entries
    # take note of our primary key "flow_id", this is used to resolve
    # conflicts in step 4

    satori_create_table = "CREATE TABLE if not exists "
    satori_create_table += postgres_schema_name + "." + postgres_table_name
    satori_create_table += """
    (flow_timestamp timestamp, account_id varchar, data_store_id varchar, 
    flow_id varchar constraint table_name_audit_data_pk primary key,
    data_store_type varchar, data_store_name varchar, identity_name varchar, identity_role varchar,
    tool varchar, locations_location varchar, queries_value bigint, volume_value bigint,
    incident_ids varchar, tags_name varchar, source varchar, records_value bigint,
    result_set varchar, result_set_column_name varchar, query_original_query varchar,
    datasets_id varchar, datasets_name varchar, query_query_type varchar, query_meta_data varchar, 
    query_meta_data_error varchar, actions_type varchar, snowflake_query_id varchar, 
    snowflake_warehouse_name varchar, athena_query_execution_id varchar, actions_policy_names varchar, 
    geo_location_attrs_country_name varchar, geo_location_attrs_city_name varchar, 
    geo_location_attrs_timezone varchar, geo_location_attrs_client_ip_str varchar, 
    identity_authentication_method varchar)
    """
    try:
        cursor.execute(satori_create_table)
        conn.commit()
    except Exception as err:
        print("Oops! An exception has occured:", err)
        print("Exception TYPE:", type(err))


    ############################################################
    # STEP 2: create temp table

    satori_create_temp_table_sql = "CREATE TABLE if not exists "
    satori_create_temp_table_sql += postgres_schema_name + ".satori_audit_tempbuffer"
    satori_create_temp_table_sql += """
    (flow_timestamp timestamp, account_id varchar, data_store_id varchar, flow_id varchar,
    data_store_type varchar, data_store_name varchar, identity_name varchar, identity_role varchar,
    tool varchar, locations_location varchar, queries_value bigint, volume_value bigint,
    incident_ids varchar, tags_name varchar, source varchar, records_value bigint,
    result_set varchar, result_set_column_name varchar, query_original_query varchar,
    datasets_id varchar, datasets_name varchar, query_query_type varchar, query_meta_data varchar, 
    query_meta_data_error varchar, actions_type varchar, snowflake_query_id varchar, 
    snowflake_warehouse_name varchar, athena_query_execution_id varchar, actions_policy_names varchar, 
    geo_location_attrs_country_name varchar, geo_location_attrs_city_name varchar, 
    geo_location_attrs_timezone varchar, geo_location_attrs_client_ip_str varchar, 
    identity_authentication_method varchar)
    """

    try:
        cursor.execute(query=satori_create_temp_table_sql)
        conn.commit()
    except Exception as err:
        print("Oops! An exception has occured:", err)
        print("Exception TYPE:", type(err))


    ############################################################
    # STEP 3: load from Satori API to temp table

    satori_copy_to_buffer = "COPY " + postgres_schema_name + ".satori_audit_tempbuffer" + " FROM stdin WITH CSV HEADER DELIMITER ','"

    try:
        cursor.copy_expert(sql=satori_copy_to_buffer, file=getAuditLogs(audit_days_ago_to_yesterday))
        conn.commit()
    except Exception as err:
        print("Oops! An exception has occured:", err)
        print("Exception TYPE:", type(err))

        #if this step fails, let's delete our temp table
        satori_drop_temp_table_sql = "DROP TABLE "
        satori_drop_temp_table_sql += postgres_schema_name + ".satori_audit_tempbuffer"
        cursor.execute(query=satori_drop_temp_table_sql)
        conn.commit()


    ############################################################
    # STEP 4: insert from temp table into final table

    satori_insert_sql = "INSERT into " + postgres_schema_name + "." + postgres_table_name
    satori_insert_sql += """
    (flow_timestamp, account_id, data_store_id, flow_id, data_store_type, data_store_name,
    identity_name, identity_role, tool, locations_location, queries_value, volume_value,
    incident_ids, tags_name, source, records_value, result_set, result_set_column_name,
    query_original_query, datasets_id, datasets_name, query_query_type, query_meta_data,
    query_meta_data_error, actions_type, snowflake_query_id, snowflake_warehouse_name,
    athena_query_execution_id, actions_policy_names, geo_location_attrs_country_name,
    geo_location_attrs_city_name, geo_location_attrs_timezone, 
    geo_location_attrs_client_ip_str, identity_authentication_method)
    select
    flow_timestamp, account_id, data_store_id, flow_id, data_store_type, data_store_name,
    identity_name, identity_role, tool, locations_location, queries_value, volume_value,
    incident_ids, tags_name, source, records_value, result_set, result_set_column_name,
    query_original_query, datasets_id, datasets_name, query_query_type, query_meta_data,
    query_meta_data_error, actions_type, snowflake_query_id, snowflake_warehouse_name,
    athena_query_execution_id, actions_policy_names, geo_location_attrs_country_name,
    geo_location_attrs_city_name, geo_location_attrs_timezone, 
    geo_location_attrs_client_ip_str, identity_authentication_method
    from satori_audit_tempbuffer ON CONFLICT (flow_id) DO NOTHING;
    """

    try:
        cursor.execute(query=satori_insert_sql)
        conn.commit()
    except Exception as err:
        print("Oops! An exception has occured:", err)
        print("Exception TYPE:", type(err))

    ############################################################
    # STEP 5: drop our temp table

    satori_drop_temp_table_sql = "DROP TABLE "
    satori_drop_temp_table_sql += postgres_schema_name + ".satori_audit_tempbuffer"

    try:
        cursor.execute(query=satori_drop_temp_table_sql)
        conn.commit()
        conn.close()
        print("We got to the end, it all seems to have worked out")
    except Exception as err:
        print("Oops! An exception has occured:", err)
        print("Exception TYPE:", type(err))
        conn.close()