import time
from api.databricks_requests import AuthorizedRequests
from errors import ClusterInfoError, ContextCreationError, ContextDeletionError, RunCommandError


class DatabricksApiHandler:
    def __init__(self, token: str, host: str, cluster_id: str):
        self._token = token
        self._host = host
        self._cluster_id = cluster_id
        self.databricks = AuthorizedRequests(token, host)

    def get_cluster_status(self) -> str:
        url = "api/2.0/clusters/get"
        response = self.databricks.get(
            url, json={"cluster_id": self._cluster_id})

        if ('state' in response.json()):
            return response.json()['state']
        else:
            if ('error_code' in response.json()):
                error_code = response.json()['error_code']
                error_message = response.json()['message']
                raise ClusterInfoError(f"{error_code}: {error_message}")
            else:
                raise ClusterInfoError("Error getting cluster info")

    def create_context(self) -> str:
        url = "api/1.2/contexts/create"
        response = self.databricks.post(
            url, json={"cluster_id": self._cluster_id})

        if ('id' in response.json()):
            return response.json()['id']
        else:
            raise ContextCreationError("Error creating context")

    def get_context_status(self, context_id: str) -> str:
        url = "api/1.2/contexts/status"
        response = self.databricks.get(
            url, json={"context_id": context_id})

        return response.json()['status']

    def delete_context(self) -> None:
        url = "api/1.2/contexts/destroy"
        response = self.databricks.post(
            url, json={"context_id": self.context_id})

        if (response.status_code != 200):
            raise ContextDeletionError("Error deleting context")
        else:
            return

    def send_command(self, context_id: str, command: str) -> str:
        url = "api/1.2/commands/execute"
        response = self.databricks.post(
            url, json={"context_id": context_id, "command": command, "language": "python", "cluster_id": self._cluster_id})

        if ('id' in response.json()):
            return response.json()['id']
        else:
            raise RunCommandError("Error running command")

    def get_command_result(self, command_id: str, context_id: str) -> str:
        url = f"api/1.2/commands/status?command_id={command_id}&cluster_id={self._cluster_id}&context_id={context_id}"
        response = self.databricks.get(
            url)

        if ('status' not in response.json()):
            raise RunCommandError("Error getting command status")
        return response.json()

    def run_command(self, context_id: str, command: str) -> dict:
        command_id = self.send_command(context_id, command)

        # check the status for the command. while the status is not finished, keep checking (max 10 times)
        # if the status is finished, return the result
        # wait each time for 5 seconds
        RETRIES = 0
        status = self.get_command_result(command_id, context_id)
        while (status['status'] != "Finished"):
            status = self.get_command_result(command_id, context_id)
            if (status['status'] == "Error"):
                raise RunCommandError("Error running command")
            RETRIES += 1
            if (RETRIES == 10):
                raise RunCommandError("Command took too long to run")
            time.sleep(5)

        return status["results"]["data"]
