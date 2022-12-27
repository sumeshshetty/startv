import awswrangler as wr
from awsglue.utils import getResolvedOptions

import pytz
import sys
import boto3
from datetime import datetime,timedelta
#import datetime
import botocore
from dateutil.tz import tzlocal

org_client= boto3.client('organizations')
tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)


#for mandatory parameters
args = getResolvedOptions(sys.argv,[])

#for optional parameters
if ('--{}'.format('month') in sys.argv) and ('--{}'.format('year') in sys.argv) :
    args_opt = getResolvedOptions(sys.argv, ['month','year'])
    args['month'] = args_opt['month']
    args['year'] = args_opt['year']
else:
    #logger.info("setting optional parameters")
    args['month']=str(current_date.month).lstrip("0")
    args['year']=str(current_date.year)


def get_account_name(line_item_usage_account_id):
    
    
    try:
        response = org_client.describe_account(
        AccountId=line_item_usage_account_id
    )
        #print(f"for {line_item_usage_account_id} got {response['Account']['Name']}")
        return response['Account']['Name']
    except Exception as e:
        print(f"not found account name for {line_item_usage_account_id}")
        return ''

args['raw_table']='raw_cost_and_usage_report'

args['raw_database']='cost_viz'


args['s3_output_path']='s3://start-detailed-billing-report/cost_visualization/processed/dimensions/account_dim/'

mode='overwrite'



athena_df=wr.athena.read_sql_query(f''' SELECT bill_payer_account_id, 
line_item_usage_account_id,
line_item_usage_account_id AS aws_account_dim_id
FROM {args['raw_table']}
GROUP BY  bill_payer_account_id,line_item_usage_account_id,line_item_usage_account_id
''', database=args['raw_database'])



athena_df.drop_duplicates(keep='first',inplace=True)
#athena_df['account_name'] = athena_df['line_item_usage_account_id'].apply(get_account_name)




if (athena_df.shape[0]) > 0:
    wr.s3.to_parquet(
    df=athena_df,
    path=args['s3_output_path'],
    dataset=True,
    mode=mode
    )
else:
    print("no new records found")
      
    
        
    
    
    
        
    
        
    
