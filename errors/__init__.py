class ContextCreationError(Exception):
    """Raised when there is an error creating the context"""
    pass


class ClusterInfoError(Exception):
    """Raised when there is an error getting cluster info"""
    pass


class ContextDeletionError(Exception):
    """Raised when there is an error deleting the context"""
    pass


class RunCommandError(Exception):
    """Raised when there is an error running a command"""
    pass
