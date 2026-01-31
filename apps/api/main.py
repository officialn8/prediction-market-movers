"""
Prediction Market Movers - API Service

FastAPI backend for SaaS platform:
- User authentication (JWT)
- Subscription management
- Market data endpoints
- User alerts & watchlists
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from apps.api.routers import auth, markets, alerts, users, webhooks, system
from packages.core.storage import get_db_pool

security = HTTPBearer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize resources on startup."""
    # Initialize DB pool
    db = get_db_pool()
    yield
    # Cleanup if needed


app = FastAPI(
    title="Prediction Market Movers API",
    description="Real-time prediction market analytics and alerts",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",  # Local dev
        "https://predictionmarketmovers.com",  # Production
        "https://*.vercel.app",  # Preview deployments
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(auth.router, prefix="/auth", tags=["Authentication"])
app.include_router(users.router, prefix="/users", tags=["Users"])
app.include_router(markets.router, prefix="/markets", tags=["Markets"])
app.include_router(alerts.router, prefix="/alerts", tags=["Alerts"])
app.include_router(webhooks.router, prefix="/webhooks", tags=["Webhooks"])
app.include_router(system.router, prefix="/system", tags=["System"])


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "api"}


@app.get("/")
async def root():
    """API root."""
    return {
        "name": "Prediction Market Movers API",
        "version": "1.0.0",
        "docs": "/docs",
    }
