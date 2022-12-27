import awswrangler as wr
from awsglue.utils import getResolvedOptions
import datetime
import pytz
import sys
import hashlib
import pandas as pd
import boto3
tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.datetime.now(tz)



#for mandatory parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# #for optional parameters
# if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) :
#     args_opt = getResolvedOptions(sys.argv, ['month','year'])
#     args['month'] = args_opt['month']
#     args['year'] = args_opt['year']
# else:
#     print("setting optional parameters")
#     args['month']=str(current_date.month).lstrip("0")
#     args['year']=str(current_date.year)


# args['raw_table']='summary_view'
# args['raw_database']='cost_viz'
# args['s3_output_path']='s3://start-detailed-billing-report/cost_visualization/processed/dimensions/resource_dim/'

# print("################## args ###########")
# print(args)
# print("################## args ###########")

# ec2 = boto3.client('ec2')



# def update_reource_name(val):
#     val=val.rsplit(':',1)
#     if len(val)>1:
#         return val[1]
#     return val[0]
    


# def create_sha(athena_df):
    
    
#     athena_df = athena_df.reindex(sorted(athena_df.columns), axis=1)
#     athena_df_colms=athena_df.columns
    
    
    
#     athena_df['concated_clms']=athena_df[athena_df_colms].apply(lambda row: '||'.join(row.values.astype(str)), axis=1)
#     athena_df['aws_product_dim_id'] = athena_df['concated_clms'].apply(lambda x: hashlib.sha256(x.encode()).hexdigest())
#     athena_df['resource_id'] = athena_df['line_item_resource_id'].apply(lambda x:update_reource_name(x) )
    
    
#     athena_df=athena_df.drop("concated_clms",axis=1)
#     return athena_df

# def get_ec2_ip(resource_name):
    
#     try:
#         ec2_response = ec2.describe_instances(InstanceIds=[resource_name
#             ])
#         return ec2_response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
#     except:
#         return ''

# def get_ec2_name(resource_name):
#     try:
#         ec2_response = ec2.describe_instances(InstanceIds=[resource_name])
#         print("ec2_response",ec2_response)
#         print("ec2 name:",ec2_response['Reservations'][0]['Instances'][0]['Tags'] )
#         tags=ec2_response['Reservations'][0]['Instances'][0]['Tags']
#         for tag in tags:
#             if tag['Key']=='Name':
#                 resource_name_2=tag['Value']
#         print("resource_name is:",resource_name_2)
#         return resource_name_2
#     except Exception as e:
#         print(e)
#         return ''
    
    
# def get_snapshot_attribute(resource_name):
    
#     try:
#         snapshot_id=resource_name.split('/')[1]
#         snapshot_response = ec2.describe_snapshots(SnapshotIds=[snapshot_id])
#         print("snap owner id",str(snapshot_response['Snapshots'][0]['RestoreExpiryTime']))
#         return str(snapshot_response['Snapshots'][0]['RestoreExpiryTime'])
#     except Exception as e:
#         print(e)
#         return ''
    
        
# def get_volume_attribute(resource_name):
#     try:
        
#         volume_response = ec2.describe_volumes(VolumeIds=[resource_name])
#         return volume_response['Volumes'][0]['VolumeType']
#     except:
#         return ''
    
# def get_secondary_identifier(resource_name, product_servicename):
#     if product_servicename == 'Amazon Elastic Compute Cloud':
#         if "snap-" in resource_name:
#             return get_snapshot_attribute(resource_name)
#         if "i-" in resource_name:
#             return get_ec2_ip(resource_name)
    
    
# def get_resource_name(resource_name, product_servicename):
#     if product_servicename == 'Amazon Elastic Compute Cloud':
#         if "i-" in resource_name:
#             return get_ec2_name(resource_name)
    


# athena_df=wr.athena.read_sql_query(f''' SELECT  line_item_resource_id, product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
# FROM {args['raw_table']}
# GROUP BY  line_item_resource_id,product_product_family, product_region, product_servicecode, product_servicename, line_item_operation,line_item_product_code
# ''', database=args['raw_database'],chunksize=True)


# athena_df_raw_list=[]
# for df in athena_df:
    
#     df=create_sha(df)
#     athena_df_raw_list.append(df)

# df_concat=pd.concat(athena_df_raw_list)
# df_concat['secondary_identifier'] = df_concat.apply(lambda x: get_secondary_identifier(x['resource_id'], x['product_servicename']), axis=1)

# df_concat['resource_name'] = df_concat.apply(lambda x: get_resource_name(x['resource_id'], x['product_servicename']), axis=1)

# df_concat.drop_duplicates(keep='first',inplace=True)


# #wr.s3.delete_objects(args['s3_output_path'])

# if (df_concat.shape[0]) > 0:
#     print("puutinf to s3..")
#     wr.s3.to_parquet(
#     df=df_concat,
#     path=args['s3_output_path'],
#     dataset=True,
#     mode="append")
# else:
#     print("error writing to s3")
        
        
        