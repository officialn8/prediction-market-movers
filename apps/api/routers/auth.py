"""
Authentication router - JWT-based auth with email/password.
"""

import os
from datetime import datetime, timedelta
from typing import Optional

import bcrypt
import jwt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, field_validator
from fastapi import Request

from packages.core.storage import get_db_pool
from apps.api.main import limiter

router = APIRouter()
security = HTTPBearer()

# Config - JWT_SECRET must be set in production
JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    raise RuntimeError("JWT_SECRET environment variable is required")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24 * 7  # 1 week


# ============================================================================
# Models
# ============================================================================

class UserRegister(BaseModel):
    email: EmailStr
    password: str
    name: Optional[str] = None
    
    @field_validator('password')
    @classmethod
    def validate_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain an uppercase letter')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain a digit')
        return v


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: dict


class UserResponse(BaseModel):
    id: str
    email: str
    name: Optional[str]
    tier: str
    created_at: datetime


# ============================================================================
# Helpers
# ============================================================================

def hash_password(password: str) -> str:
    """Hash password with bcrypt."""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    """Verify password against hash."""
    return bcrypt.checkpw(password.encode(), hashed.encode())


def create_token(user_id: str, email: str) -> tuple[str, int]:
    """Create JWT token. Returns (token, expires_in_seconds)."""
    expires = datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    payload = {
        "sub": user_id,
        "email": email,
        "exp": expires,
        "iat": datetime.utcnow(),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)
    return token, JWT_EXPIRATION_HOURS * 3600


def decode_token(token: str) -> dict:
    """Decode and validate JWT token."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> dict:
    """Dependency to get current authenticated user."""
    payload = decode_token(credentials.credentials)
    
    db = get_db_pool()
    user = db.execute(
        "SELECT * FROM users WHERE id = %s",
        (payload["sub"],),
        fetch=True
    )
    
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user[0]


# ============================================================================
# Endpoints
# ============================================================================

@router.post("/register", response_model=TokenResponse)
@limiter.limit("5/minute")
async def register(request: Request, data: UserRegister):
    """Register a new user."""
    db = get_db_pool()
    
    # Check if email exists
    existing = db.execute(
        "SELECT id FROM users WHERE email = %s",
        (data.email,),
        fetch=True
    )
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    # Create user
    hashed = hash_password(data.password)
    result = db.execute(
        """
        INSERT INTO users (email, password_hash, name, tier)
        VALUES (%s, %s, %s, 'free')
        RETURNING id, email, name, tier, created_at
        """,
        (data.email, hashed, data.name),
        fetch=True
    )
    
    user = result[0]
    token, expires_in = create_token(str(user["id"]), user["email"])
    
    return TokenResponse(
        access_token=token,
        expires_in=expires_in,
        user={
            "id": str(user["id"]),
            "email": user["email"],
            "name": user["name"],
            "tier": user["tier"],
        }
    )


@router.post("/login", response_model=TokenResponse)
@limiter.limit("5/minute")
async def login(request: Request, data: UserLogin):
    """Login with email and password."""
    db = get_db_pool()
    
    user = db.execute(
        "SELECT * FROM users WHERE email = %s",
        (data.email,),
        fetch=True
    )
    
    if not user or not verify_password(data.password, user[0]["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    user = user[0]
    token, expires_in = create_token(str(user["id"]), user["email"])
    
    return TokenResponse(
        access_token=token,
        expires_in=expires_in,
        user={
            "id": str(user["id"]),
            "email": user["email"],
            "name": user["name"],
            "tier": user["tier"],
        }
    )


@router.get("/me", response_model=UserResponse)
async def get_me(user: dict = Depends(get_current_user)):
    """Get current user profile."""
    return UserResponse(
        id=str(user["id"]),
        email=user["email"],
        name=user["name"],
        tier=user["tier"],
        created_at=user["created_at"],
    )


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(user: dict = Depends(get_current_user)):
    """Refresh JWT token."""
    token, expires_in = create_token(str(user["id"]), user["email"])
    
    return TokenResponse(
        access_token=token,
        expires_in=expires_in,
        user={
            "id": str(user["id"]),
            "email": user["email"],
            "name": user["name"],
            "tier": user["tier"],
        }
    )
