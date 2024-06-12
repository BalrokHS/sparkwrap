from api import DatabricksApiHandler
from parse import DatabricksCommandBuilder


class SparkWrapper:
    def __init__(self, token: str, host: str, cluster_id: str):
        self._token = token
        self._host = host
        self._cluster_id = cluster_id
        self.databricks = DatabricksApiHandler(token, host, cluster_id)

    def command_builder():
        return DatabricksCommandBuilder()

    def run(self, command: str):
        context_id = self.databricks.create_context()
        result = self.databricks.run_command(context_id, command)
        self.databricks.delete_context()
        return result
