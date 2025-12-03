# api/rate_limit.py (or utils/rate_limit.py)

from functools import wraps
from flask import request

def rate_limit_stub(max_requests: int = 100, window_seconds: int = 60):
    """
    Stub decorator for endpoint rate limiting.

    Args:
        max_requests: Maximum number of allowed requests (placeholder).
        window_seconds: Time window in seconds (placeholder).

    This does NOT enforce real rate limiting yet. It only wraps the view
    so we can later plug in a real implementation (e.g., Flask-Limiter).
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            # TODO: integrate actual rate limiting here.
            client_ip = request.remote_addr
            # Log / debug info could go here if desired.
            return fn(*args, **kwargs)
        return wrapper
    return decorator