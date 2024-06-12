import ast
import inspect
import re
import textwrap
from abc import ABC, abstractmethod


class ReturnRemover(ast.NodeTransformer):
    def visit_Return(self, node):
        return


class AbstarctDatabricksCommandInterface(ABC):
    NEW_LINE_LITERAL = "\n"

    def __init__(self):
        self.command = ""

    def build_import_commands(self, import_commands: list[str]):
        for import_command in import_commands:
            self.command += import_command + self.NEW_LINE_LITERAL

    """
    Add classes and methods that are necessary for the implementation
    of the service layer of the script, that will be run on the Databricks
    cluster.
    """

    def build_command_service_layer(self, *implementations):
        for implementation in implementations:
            self.command += inspect.getsource(implementation) + \
                self.NEW_LINE_LITERAL

    # TODO: Add a method that will recognize the return variable of the 'build_main_script'
    # and will add a write command to databricks.

    """
    Implement this method. This will be the main script that will use 
    all the above classes, methods, and imports.
    """

    @abstractmethod
    def build_main_script(self, *args):
        raise NotImplementedError

    @staticmethod
    def replace_args_in_ast(node, arg_values):
        for child in ast.walk(node):
            if isinstance(child, ast.Name) and child.id in arg_values:
                child.id = repr(arg_values[child.id])

    @staticmethod
    def clean_output_implementation(input_string):
        # Use a regular expression to remove the first line
        modified_string = re.sub(
            r'^.*\n', '', input_string, count=1, flags=re.MULTILINE)

        # Use another regular expression to remove one tab (or four spaces) from the start of each line
        modified_string = re.sub(
            r'^(\t| {4})', '', modified_string, flags=re.MULTILINE)

        return modified_string

    def build_main_script_implementation(self, function, *args, **kwargs):
        # Get the source code of the function
        source_code = inspect.getsource(function)

        # Parse the source code into an AST
        parsed_source = ""

        try:
            parsed_source = ast.parse(source_code)
        except IndentationError:
            parsed_source = ast.parse(textwrap.dedent(source_code))

        parsed_source = ReturnRemover().visit(parsed_source)

        # Combine args and kwargs, and handle defaults
        args_names = inspect.getfullargspec(function).args

        args_names.remove('self')
        args_names.remove('spark')

        defaults = inspect.getfullargspec(function).defaults or ()
        default_args = dict(zip(args_names[-len(defaults):], defaults))
        all_args = dict(zip(args_names, args), **kwargs)
        # Override defaults with actual args
        all_args = {**default_args, **all_args}

        # Replace the argument variables in the AST
        self.replace_args_in_ast(parsed_source, all_args)

        # Convert the AST back to source code
        modified_source_code = ast.unparse(parsed_source)

        self.command += self.clean_output_implementation(modified_source_code)
