import awswrangler as wr
from awsglue.utils import getResolvedOptions
import datetime
import pytz
import sys
import hashlib
import pandas as pd
import boto3
import sys
import awswrangler as wr
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)


spark = SparkSession.builder.config("spark.sql.sources.partitionOverwriteMode","dynamic").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
logger = glueContext.get_logger()
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
args['s3_output_path']='s3://start-detailed-billing-report/cost_visualization/processed/dimensions/resource_dim_spark/'
args['s3_temp_path']='s3://start-detailed-billing-report/cost_visualization/cv_temp_2'
print("################## args ###########")
print(args)
print("################## args ###########")

ec2 = boto3.client('ec2')



def update_resource_name(str):
    val=str
    val=val.rsplit(':',1)
    if len(val)>1:
        return val[1]
    return val[0]
    


def get_ec2_ip(resource_name):
    resource_name=str(resource_name)
    
    try:
        ec2_response = ec2.describe_instances(InstanceIds=[resource_name
            ])
        return ec2_response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
    except:
        return ''
        

        


def get_ec2_name(resource_name):
    resource_name=str(resource_name)
    try:
        ec2_response = ec2.describe_instances(InstanceIds=[resource_name])
        print("ec2_response",ec2_response)
        print("ec2 name:",ec2_response['Reservations'][0]['Instances'][0]['Tags'] )
        tags=ec2_response['Reservations'][0]['Instances'][0]['Tags']
        for tag in tags:
            if tag['Key']=='Name':
                resource_name_2=tag['Value']
        print("resource_name is:",resource_name_2)
        return resource_name_2
    except Exception as e:
        print(e)
        return ''
  
def update_secondary_identifier(a, b):
    if a == 'Amazon Elastic Compute Cloud':
        if "i-" in str(b):
            return get_ec2_ip(str(b))
    
    
    
def generate_sha(str):
	return sha2(str,256)

def update_ec2_name(resource_name, product_servicename):
    if str(product_servicename) == 'Amazon Elastic Compute Cloud':
        if "i-" in str(resource_name):
            return get_ec2_name(str(resource_name))
            
            
wr.s3.delete_objects(args['s3_output_path'])




athena_view_dataframe = (
    glueContext.read.format("jdbc")
    .option("mode","overwrite")
    .option("AwsCredentialsProviderClass","com.simba.athena.amazonaws.auth.InstanceProfileCredentialsProvider")
    .option("driver", "com.simba.athena.jdbc.Driver")
    .option("url", "jdbc:awsathena://athena.ap-south-1.amazonaws.com:443")
    .option("dbtable", f"{args['raw_database']}.summary_view")
    .option("S3OutputLocation",args['s3_temp_path']) # CSVs/metadata dumped here on load
    .load()
    )



athena_view_datasource = DynamicFrame.fromDF(athena_view_dataframe, glueContext, "athena_view_source")
athena_view_datasource_df=athena_view_datasource.toDF()

athena_view_datasource_df.createOrReplaceTempView("summary_view")


filtered_list=['line_item_resource_id', 'product_product_family', 'product_region', 'product_servicecode', 'product_servicename', 'line_item_operation','line_item_product_code']


filtered_list.sort()
colnm_str=','.join(filtered_list)


query=''' SELECT {} FROM summary_view GROUP BY {}'''.format(colnm_str,colnm_str)
cost_insights_df=spark.sql(query)


cost_insights_df=cost_insights_df.withColumn('concated_tags',concat_ws("||",*filtered_list))

cost_insights_df=cost_insights_df.withColumn('aws_product_dim_id',generate_sha(cost_insights_df.concated_tags))
cost_insights_df=cost_insights_df.drop("concated_tags")


update_resource_name_UDF = udf(lambda z:update_resource_name(z),StringType())
cost_insights_df=cost_insights_df.withColumn("resource_id", update_resource_name_UDF(col("line_item_resource_id")))


update_secondary_identifier_udf = udf(update_secondary_identifier, StringType())
cost_insights_df=cost_insights_df.withColumn("secondary_identifier", update_secondary_identifier_udf(col("resource_id"), col("product_servicename")))

# update_ec2_name_udf = udf(update_ec2_name, StringType())
# cost_insights_df=cost_insights_df.withColumn("resource_name", update_ec2_name_udf(col("resource_id"), col("product_servicename")))


cost_insights_df.show(truncate=False)

cost_insights_df.write.mode("overwrite").parquet(args['s3_output_path'])
job.commit()

        
        
        