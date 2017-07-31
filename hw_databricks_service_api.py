import os
from databricks_cli.sdk import *  # pylint: disable=unused-wildcard-import,wildcard-import
from databricks_cli.configure.config import DatabricksConfig

config = DatabricksConfig.fetch_from_fs()
api_stub = ApiClient(config.username, config.password, host=config.host)

cluster_api = ClusterService(api_stub)
cluster_api.list_available_zones()
# cluster_api.list_clusters()
cluster_api.list_node_types()
cluster_api.list_spark_versions()

ret = cluster_api.list_clusters()
clusters = ret['clusters']

# Load SSH public key for remote access
_SSH_PUBLIC_KEY_FNAME = os.path.join(os.environ['HOME'], '.ssh', 'databricks_cluster.pub')
with open(_SSH_PUBLIC_KEY_FNAME, 'r') as fin:
    SSH_PUBLIC_KEY = fin.read()

# https://docs.databricks.com/api/latest/clusters.html#create
cluster = cluster_api.create_cluster(
    num_workers=0,
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

SSH_DRIVER_TEMPLATE = """
#!/bin/bash

exec ssh \
     -i ~/.ssh/databricks_cluster \
     -oStrictHostKeyChecking=no \
     -L 6009:localhost:6006 \
     -p 2200 \
     -At \
     "ubuntu@{host_public_dns}"
"""

script_fname = 'with_gpu_cloud.sh'
with open(script_fname, 'w') as fout:
    _script = SSH_DRIVER_TEMPLATE.format(host_public_dns=driver_ssh_host)
    fout.write(_script)

os.chmod(script_fname, 0o755)

# Run the following to destroy the cluster
# ret = cluster_api.delete_cluster(cluster['cluster_id'])

"""
The DBFS API
"""
dbfs_api = DbfsService(api_stub)
dbfs_api.list('dbfs:/ml/build')


"""
The Library API
"""
library_api = LibraryService(api_stub)

ret = library_api.client.perform_query(
    'GET', '/libraries/list')

ret = library_api.client.perform_query(
    'GET', '/libraries/all-cluster-statuses')

ret = library_api.client.perform_query(
    'GET', '/libraries/cluster-status', data={
        'cluster_id': cluster['cluster_id']
    })

ret = library_api.client.perform_query(
    'POST', '/libraries/install', data={
        'cluster_id': cluster['cluster_id'],
        'libraries': [
            {'pypi': {
                'package': 'pytorch',
                'repo': 'http://download.pytorch.org/whl/cu80/torch-0.1.12.post2-cp35-cp35m-linux_x86_64.whl'
            }}
        ]
    })
