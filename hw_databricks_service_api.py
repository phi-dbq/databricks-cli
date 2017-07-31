from databricks_cli.sdk import *  # pylint: disable=unused-wildcard-import,wildcard-import
from databricks_cli.configure.config import DatabricksConfig

config = DatabricksConfig.fetch_from_fs()
client = ApiClient(config.username, config.password, host=config.host)

cluster_api = ClusterService(client)
cluster_api.list_available_zones()
cluster_api.list_clusters()

ret = cluster_api.list_clusters()
clusters = ret['clusters']
