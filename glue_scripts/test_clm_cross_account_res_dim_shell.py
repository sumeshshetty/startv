import awswrangler as wr
from awsglue.utils import getResolvedOptions
import datetime
import pytz
import sys
import hashlib
import pandas as pd
import boto3
import botocore
from dateutil.tz import tzlocal
from datetime import datetime,timedelta
import os
pd.set_option('display.max_colwidth', None)

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)
os.environ['AWS_STS_REGIONAL_ENDPOINTS']='legacy'


#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
else:
    print("setting optional parameters")
    args['month']=str(current_date.month).lstrip("0")
    args['year']=str(current_date.year)


args['raw_table']='summary_view'
args['raw_database']='cost_viz'
args['s3_output_path']='s3://start-detailed-billing-report/cost_visualization/processed/dimensions/resource_dim/'
#athena_df_colms =['line_item_resource_id' ,'line_item_operation', 'line_item_product_code', 'product_product_family', 'product_region', 'product_servicecode', 'product_servicename']
athena_df_colms = ['line_item_operation', 'line_item_product_code', 'line_item_resource_id', 'product_product_family', 'product_region', 'product_servicecode', 'product_servicename']

assume_role_cache: dict = {}
def assumed_role_session(role_arn: str, base_session: botocore.session.Session = None):
    base_session = base_session or boto3.session.Session()._session
    fetcher = botocore.credentials.AssumeRoleCredentialFetcher(
        client_creator = base_session.create_client,
        source_credentials = base_session.get_credentials(),
        role_arn = role_arn,
        extra_args = {}
    )
    creds = botocore.credentials.DeferredRefreshableCredentials(
        method = 'assume-role',
        refresh_using = fetcher.fetch_credentials,
        time_fetcher = lambda: datetime.now(tzlocal())
    )
    botocore_session = botocore.session.Session()
    botocore_session._credentials = creds
    return boto3.Session(botocore_session = botocore_session)
    


def get_ec2_client(account_id):
    print(f"got account {account_id}")
    if account_id !='792073293430':
        print(f"for {account_id} assuming role : arn:aws:iam::{account_id}:role/CLM-CrossAccountRole")
        session = assumed_role_session(f'arn:aws:iam::{account_id}:role/CLM-CrossAccountRole')
        print("succesfully created session")
        return session.client('ec2')
    else:
        return boto3.client('ec2')
    
    

def get_ec2_ip(resource_name, account_id):
    ec2 = get_ec2_client(account_id)
    try:
        ec2_response = ec2.describe_instances(InstanceIds=[resource_name
            ])
        print(f"ip addrss for {resource_name} is {ec2_response['Reservations'][0]['Instances'][0]['PrivateIpAddress']} in {account_id} ")
        return ec2_response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
    except Exception as e:
        print("exception in getce2ip",e,"for account", account_id)
        return ''
        

def get_secondary_identifier(resource_name, product_servicename, account_id):
    if product_servicename == 'Amazon Elastic Compute Cloud':
        # if "snap-" in resource_name:
        #     return get_snapshot_attribute(resource_name)
        if "i-" in resource_name:
            return get_ec2_ip(resource_name, account_id)

def get_ec2_name(resource_name, account_id):
    ec2 = get_ec2_client(account_id)
    try:
        ec2_response = ec2.describe_instances(InstanceIds=[resource_name])
        
        print("ec2 name:",ec2_response['Reservations'][0]['Instances'][0]['Tags'] )
        tags=ec2_response['Reservations'][0]['Instances'][0]['Tags']
        for tag in tags:
            if tag['Key']=='Name':
                resource_name_2=tag['Value']
        print("resource_name is:",resource_name_2,"for account id:",account_id)
        return resource_name_2
    except Exception as e:
        print(e)
        return ''

def get_resource_name(resource_name, product_servicename, account_id):
    if product_servicename == 'Amazon Elastic Compute Cloud':
        if "i-" in resource_name:
            return get_ec2_name(resource_name, account_id)


def update_resource_name(val):
    val=val.rsplit(':',1)
    if len(val)>1:
        return val[1]
    return val[0]

def create_sha(athena_df):
    
    
    athena_df = athena_df.reindex(sorted(athena_df.columns), axis=1)
    #athena_df_colms=athena_df.columns
    
    
    
    athena_df['concated_clms']=athena_df[athena_df_colms].apply(lambda row: '||'.join(row.values.astype(str)), axis=1)
    
    athena_df['aws_product_dim_id'] = athena_df['concated_clms'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
    athena_df['resource_id'] = athena_df['line_item_resource_id'].apply(lambda x:update_resource_name(x) )
    
    
    athena_df=athena_df.drop("concated_clms",axis=1)
    return athena_df


#in list ('831083831535','664182639337','542251324780','326792458540','792073293430')
athena_df=wr.athena.read_sql_query(f''' SELECT  
line_item_resource_id, product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code,
linked_account_id
FROM {args['raw_table']} where linked_account_id in ('831083831535','542251324780')
and line_item_operation = 'RunInstances'
GROUP BY  
line_item_resource_id,product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code,
linked_account_id
''', database=args['raw_database'],chunksize=True)


athena_df_raw_list=[]
for df in athena_df:
    
    df=create_sha(df)
    athena_df_raw_list.append(df)



df_concat=pd.concat(athena_df_raw_list)


df_concat['secondary_identifier'] = df_concat.apply(lambda x: get_secondary_identifier(x['resource_id'], x['product_servicename'],x['linked_account_id']), axis=1)

df_concat['resource_name'] = df_concat.apply(lambda x: get_resource_name(x['resource_id'], x['product_servicename'],x['linked_account_id']), axis=1)


df_concat.drop_duplicates(keep='first',inplace=True)
df_concat.drop(columns=['linked_account_id'], inplace=True)

#wr.s3.delete_objects(args['s3_output_path'])

if (df_concat.shape[0]) > 0:
    print("puutinf to s3..")
    wr.s3.to_parquet(
    df=df_concat,
    path=args['s3_output_path'],
    dataset=True,
    mode="append")
else:
    print("error writing to s3")
