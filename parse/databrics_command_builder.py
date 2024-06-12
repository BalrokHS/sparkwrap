from .abstract_databricks_command_interface import AbstarctDatabricksCommandInterface


class DatabricksCommandBuilder:
    def __init__(self):
        self.command_instance = AbstarctDatabricksCommandInterface()

    def with_imports(self, import_commands: list[str]):
        self.command_instance.build_import_commands(import_commands)
        return self

    def with_service_layer(self, *implementations):
        self.command_instance.build_command_service_layer(*implementations)
        return self

    def with_main_script(self, *args):
        self.command_instance.build_main_script(*args)
        return self

    def build(self):
        return self.command_instance.command
