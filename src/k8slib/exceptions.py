class DeploymentError(Exception):
    """Raised when deployment or deletion actions fail."""


class MonitorTimeout(Exception):
    """Raised when readiness monitoring exceeds the timeout."""


class ThreadManagementError(Exception):
    """Raised when thread lifecycle operations fail."""
