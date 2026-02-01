"""
Rate limiting configuration for FastAPI.
"""

from slowapi import Limiter
from slowapi.util import get_remote_address

# Rate limiter - uses client IP by default
limiter = Limiter(key_func=get_remote_address)