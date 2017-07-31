import os
from databricks_cli.sdk import *  # pylint: disable=unused-wildcard-import,wildcard-import
from databricks_cli.configure.config import DatabricksConfig

config = DatabricksConfig.fetch_from_fs()
client = ApiClient(config.username, config.password, host=config.host)

cluster_api = ClusterService(client)
cluster_api.list_available_zones()
cluster_api.list_clusters()
cluster_api.list_node_types()
cluster_api.list_spark_versions()

ret = cluster_api.list_clusters()
clusters = ret['clusters']

_SSH_PUBLIC_KEY_FNAME = os.path.join(os.environ['HOME'], '.ssh', 'databricks_cluster.pub')
with open(_SSH_PUBLIC_KEY_FNAME, 'r') as fin:
    SSH_PUBLIC_KEY = fin.read()

# https://docs.databricks.com/api/latest/clusters.html#create
cluster = cluster_api.create_cluster(
    num_workers=2,
    autoscale=None,
    cluster_name='phi9t-pytorch-gpu',
    node_type_id='p2.xlarge',
    spark_version='2.1.x-gpu-scala2.11',
    spark_env_vars={
        'PYSPARK_PYTHON': '/databricks/python3/bin/python3'
    },
    aws_attributes={
        'availability': 'SPOT_WITH_FALLBACK',
        'ebs_volume_count': 3,
        'ebs_volume_size': 100,
        'ebs_volume_type': 'GENERAL_PURPOSE_SSD',
        'first_on_demand': 1,
        'spot_bid_price_percent': 100,
        'zone_id': 'us-west-2a'
    },
    ssh_public_keys=[SSH_PUBLIC_KEY]
)

# We can actually SSH into worker machines
cluster_info = cluster_api.get_cluster(cluster['cluster_id'])

driver_ssh_host = cluster_info['driver']['public_dns']

# We could SSH into the machines and do stuffs
import paramiko

ssh = paramiko.SSHClient()
ssh.load_system_host_keys()
ssh.load_host_keys(os.path.join(os.environ['HOME'], '.ssh', 'databricks_cluster'))
ssh.set_missing_host_key_policy(paramiko.WarningPolicy())
ssh.connect(driver_ssh_host, 2200, 'ubuntu')
print(repr(ssh.get_transport()))
chan = ssh.invoke_shell(); chan.close()
