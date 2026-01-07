import redshift_connector
import os
import boto3
from botocore.exceptions import ClientError
import json
import pandas as pd
import datetime
import csv

def redshift_conn():

    secret_name =  os.environ['RS_SECRET_NAME']
    region_name =  os.environ['RS_REGION']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    # Decrypts secret using the associated KMS key.
    rs_secret_arn=get_secret_value_response['ARN']
    secret = get_secret_value_response['SecretString']
    rs_secret = json.loads(secret)
        
    # rs_database =  rs_secret['dbname']
    rs_host = rs_secret['host']
    rs_database = rs_secret['dbname']
    rs_user = rs_secret['username']
    rs_password =rs_secret['password']

    rs_conn = redshift_connector.connect(
        host=rs_host,
        database=rs_database,
        user=rs_user,
        password=rs_password
    )
    return rs_conn

###DB SETUP###
def rs_db_setup():

    processedSQL = ('CREATE TABLE IF NOT EXISTS sample_test1.batch_processed ('
                                                                 '"flowname" CHAR(14),'
                                                                 'PRIMARY KEY("flowname"))')
    dataTblSQL = ('CREATE TABLE IF NOT EXISTS sample_test1.batch_data ('
                                                          '"c1" TIMESTAMP,'
                                                          '"c2" TIMESTAMP,'
                                                          '"c3" VARCHAR(10)')

    rs_conn = redshift_conn()
    cursor = rs_conn.cursor()
    cursor.execute(processedSQL)
    cursor.execute(dataTblSQL)
    rs_conn.commit()
    cursor.close()
    rs_conn.close()
    
    return 

def get_flow_data(load_key_query):

    print(f'Querying {load_key_query}')
    rs_conn = redshift_conn()
    cursor = rs_conn.cursor()
    cursor.execute(load_key_query)
    load_key_df = cursor.fetch_dataframe()
    cursor.close()
    rs_conn.close()
    
    return load_key_df

def insert_dataset(query,sqldata):

    rs_conn = redshift_conn()
    rs_conn.autocommit = True
    cursor = rs_conn.cursor()

    redshift_connector.paramstyle =  'format'
    try:
        cursor.executemany(query,sqldata)
        rs_conn.commit()
    except Exception as e:
        print('Error while inserting data table',e)
        rs_conn.rollback()
        raise e
    cursor.close()
    rs_conn.close()
    return

def insert_dataset_row(query,sqldata):

    rs_conn = redshift_conn()
    rs_conn.autocommit = True
    cursor = rs_conn.cursor()

    dataset = [tupl for tupl in sqldata]
    print('flowtime',dataset[0][0])

    redshift_connector.paramstyle = 'format'
    for row in dataset:
        try:
            cursor.execute(query,row)
            rs_conn.commit()
        except Exception as e:
            print('Error while inserting data row',row,e)
            rs_conn.rollback()
            raise e
    cursor.close()
    rs_conn.close()
    return

def insert_dataset_s3(rs_table,sqldata):

    dataset = [tupl for tupl in sqldata]
    wd = os.environ["IN_BUCKET"]
    rs_iamrole =os.environ["RS_IAM_ROLE"]
    
    file_name = 'parsedfile_'+datetime.datetime.today().strftime("%Y%m%d%H%M%S")+".csv"
    
    s3_filename = f"staging/{file_name}"
    print(f"Outputting {s3_filename}")

    tmp_file = '/tmp/' + file_name
    
    with open(tmp_file, "w") as f:
        csv_writer = csv.writer(f)
        for mytuple in dataset:
            csv_writer.writerow(mytuple)

    s3_client = boto3.resource('s3')
    s3_client.Bucket(wd).upload_file(tmp_file, s3_filename)

    rs_conn = redshift_conn()
    rs_conn.autocommit = True
    cursor = rs_conn.cursor()
    
    redshift_connector.paramstyle = 'format'
    load_s3_query = f"copy {rs_table} from 's3://{wd}/{s3_filename}' iam_role '{rs_iamrole}' csv;"
    try:
        cursor.execute(load_s3_query)
        rs_conn.commit()
    except Exception as e:
        print('Error while inserting redshift data',e)
        rs_conn.rollback()
        raise e
    cursor.close()
    rs_conn.close()
    return
