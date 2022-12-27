import pytz
import sys
import boto3
from datetime import datetime,timedelta
#import datetime
import botocore
from dateutil.tz import tzlocal
import os
os.environ['AWS_STS_REGIONAL_ENDPOINTS']='legacy'

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





# usage:
ins_list =['i-0ad98c2c55f6b09e1','i-0b2ed7a7bd9be239a','i-018a84c87d83ed95a']
session = assumed_role_session('arn:aws:iam::326792458540:role/CLM-CrossAccountRole')
ec2 = session.client('ec2') # ... etc.
response  = ec2.describe_instances()
print(response)