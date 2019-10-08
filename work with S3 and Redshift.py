# To add a new cell, type '#%%'
# To add a new markdown cell, type '#%% [markdown]'
#%%
#!pip install flatten_json
#!pip install pandasql
from datetime import datetime
import time 
import json
import pandas as pd
from flatten_json import flatten
from pandas.io.json import json_normalize
from pandasql import *
# use below to install psycopg2 module:
# env LDFLAGS="-I/usr/local/opt/openssl/include -L/usr/local/opt/openssl/lib" pip install psycopg2
import psycopg2 as pg
import boto3
# do some display settings 
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


#%%
#connect to PA follower db
con = pg.connect(host="##########################",
                       database="##########################", 
                       user="##########################", 
                       password="##########################")


#%%
query= """ with dim_customers as (
select distinct id as customer_id, first_name, last_name, phone_number, email, created_at, updated_at, external_id, uuid, metadata ->> 'partnersId' as partnersId 
from public.customers 
)
select * from dim_customers """

df=pd.read_sql(query, con)

# size of df 
import sys
from decimal import Decimal
memory_usage= sys.getsizeof(df)
gigabyte= 1024**3
df_size= round(memory_usage / gigabyte,3)

print(f'{df_size} GB') 


#%%
df

#%% [markdown]
# ## set your AWS account credentials using the below command and enter your Access key ID and Secret Access Key. You can use default for region and output format 
# #aws configure
# #AWS Access Key ID [****************]:
# #AWS Secret Access Key [****************]:
# #Default region name [eu-west-1]:
# #Default output format [None]:
# ## then boto3  config file (named credentials) is set up at  '~/.aws/credentials'. 

#%%
s3 = boto3.resource('s3')


#%%
for bucket in s3.buckets.all():

       print(bucket.name)


#%%
mybucket= 'cover-data-engineering'


#%%
# list all objects in a bucket
for file in s3.Bucket('cover-data-engineering').objects.all():
    print(file.key)

#%% [markdown]
# ## upload local file to S3 

#%%
#df.to_csv('/Users/Yue/work/sqloutput/policies_local.csv', index=False)

s3.Object('cover-data-engineering','policies_from_local.csv').upload_file(Filename='/Users/Yue/work/sqloutput/policies_local.csv')

#%% [markdown]
# ## another way to upload local file to S3
# #s3_client = boto3.client('s3')
# #s3_client.upload_file('/Users/Yue/work/sqloutput/policies_local.csv','cover-data-engineering','policies_from_local.csv')
#%% [markdown]
# ## upload pandas dataframe directly to s3

#%%
#now = datetime.now().strftime("%Y_%m_%d %H:%M:%S")
datestamp = datetime.now().strftime("%Y%m%d")

datestamp


#%%
df_name = 'customers'
s3_file = df_name +  '_' + datestamp + '.csv'
s3_file


#%%
# write dataframe to buffer 
from io import StringIO
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# upload it to s3:
s3.Object(mybucket, s3_file).put(Body=csv_buffer.getvalue())

#%% [markdown]
# # Upload file from S3 to Redshift

#%%
#connect to AWS Redshift Data Warehouse
dw_con = pg.connect(host="##########################",
                 port=5439,
                 database="##########################", 
                 user="##########################", 
                 password="##########################")


#%%
# truncate the table first 
cur = dw_con.cursor()
cur.execute(""" truncate table policyadmin.customers """)

#%% [markdown]
# # load the file to redshift table
# cur.execute("begin;")
# dw_con.commit()
# 
# sql = """
#     copy policyadmin.customers from 's3://cover-data-engineering/customers_20191003.csv'
#     access_key_id '##########################'
#     secret_access_key '##########################'
#     region 'us-east-1'
#     ignoreheader 1
#     removequotes
#     delimiter ',';
#     """
# cur.execute(sql)
# dw_con.commit()

#%%
# Dynamically load the S3 file to redshift table
cur.execute("begin;")
dw_con.commit()

sql =  """copy policyadmin.customers from 's3://cover-data-engineering/""" + s3_file +  "'" + """ 
    access_key_id '##########################'
    secret_access_key '##########################'
    region 'us-east-1'
    ignoreheader 1
    EMPTYASNULL
    removequotes
    delimiter ',';
    """
cur.execute(sql)
dw_con.commit()


#%%



