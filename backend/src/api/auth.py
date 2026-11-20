"""Authentication lives in ``src.api.deps``; re-exported here for discoverability."""

from src.api.deps import verify_api_key

__all__ = ["verify_api_key"]
