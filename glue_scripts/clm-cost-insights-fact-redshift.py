import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
import pytz

tz=pytz.timezone('Asia/Calcutta')
current_date=datetime.now(tz)

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
    



#for TempDir USE BASE PATH

args['raw_db_name']='cost_viz'
args['raw_table_name']='processed_cost_insights_fact'
args["TempDir"]='s3://start-detailed-billing-report/cost_visualization/Temp'

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

job = Job(glueContext)
job.init(args['JOB_NAME'], args)


db_table="cost_viz.cost_insights_fact"
db_name='dev'
stag_db_table='cost_viz.stage_cost_insights_fact'






logger.info("****using following args****")
logger.info(str(args))
logger.info("****using following args****")



datasource0 = glueContext.create_dynamic_frame_from_catalog(
    database=args['raw_db_name'],
      table_name=args['raw_table_name'])

df=datasource0.toDF()


    
df=df.drop("month","year")



       
pre_query="begin; truncate table cost_viz.cost_insights_fact; end;"
dyf = DynamicFrame.fromDF(df, glueContext, "dyf")


datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dyf, catalog_connection = "redshift-glue-connection-clm", connection_options = {"dbtable": db_table, "database":db_name,"preactions":pre_query}, redshift_tmp_dir = args["TempDir"]+"/cost_insights_fact", transformation_ctx = "datasink4")

job.commit()
