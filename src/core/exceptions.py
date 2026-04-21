class AppError(Exception):
    """Base application error."""


class DataAccessError(AppError):
    """Raised when a database or repository operation fails."""


class BusinessRuleError(AppError):
    """Raised when a business rule is violated."""


class ConfigError(AppError):
    """Raised when required application configuration is invalid."""


class AuthError(AppError):
    """Raised when authentication cannot be completed safely."""
