class RenzoError(Exception):
    """Base exception for all Renzo domain errors."""


class ProfileLoadError(RenzoError):
    """Raised when the user profile cannot be loaded or is invalid."""


class PipelineError(RenzoError):
    """Raised when a pipeline stage encounters a non-recoverable error."""


class StorageError(RenzoError):
    """Raised on unrecoverable database errors."""


class FetchError(RenzoError):
    """Raised when a job source fetch fails completely."""


class ValidationError(RenzoError):
    """Raised when a job or profile fails validation."""
