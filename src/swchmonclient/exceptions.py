class DeploymentError(Exception):
    """Raised when deployment or deletion actions fail."""


class ThreadManagementError(Exception):
    """Raised when thread lifecycle operations fail."""


class MetricSubscriptionError(Exception):
    """Raised when metric subscription lifecycle operations fail."""
