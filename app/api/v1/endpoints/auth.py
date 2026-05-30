"""
Auth endpoints — proxy to Supabase Auth.

When AUTH_ENABLED=false: returns mock responses (dev mode, no Supabase required).
When AUTH_ENABLED=true: delegates to supabase.auth.* SDK calls.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from app.auth import DEV_USER, get_current_user
from app.schemas.auth import LoginRequest, RegisterRequest, TokenResponse, UserResponse
from config.settings import settings
from utils.logger import get_logger

router = APIRouter(prefix="/auth", tags=["auth"])
logger = get_logger(__name__)

_DEV_TOKEN = "dev-mode-no-jwt-required"


def _get_supabase_client():
    """Lazy Supabase client — only instantiated when AUTH_ENABLED=true."""
    if not settings.SUPABASE_URL or not settings.SUPABASE_ANON_KEY:
        raise HTTPException(
            status_code=503,
            detail="Supabase is not configured. Set SUPABASE_URL and SUPABASE_ANON_KEY.",
        )
    try:
        from supabase import create_client
        return create_client(settings.SUPABASE_URL, settings.SUPABASE_ANON_KEY)
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="supabase package not installed. Run: pip install supabase",
        )


@router.post("/register", response_model=TokenResponse)
def register(body: RegisterRequest):
    """
    Register a new user.
    - AUTH_ENABLED=false: returns a mock dev token (no Supabase needed)
    - AUTH_ENABLED=true: creates a Supabase Auth user
    """
    if not settings.AUTH_ENABLED:
        logger.info("[AUTH] Dev mode: returning mock register response")
        return TokenResponse(
            access_token=_DEV_TOKEN,
            user_id=DEV_USER["id"],
            email=body.email,
        )

    supabase = _get_supabase_client()
    try:
        response = supabase.auth.sign_up({
            "email": body.email,
            "password": body.password,
        })
        if not response.session:
            raise HTTPException(
                status_code=400,
                detail="Registration failed — check email confirmation settings",
            )
        return TokenResponse(
            access_token=response.session.access_token,
            user_id=str(response.user.id) if response.user else None,
            email=response.user.email if response.user else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[AUTH] Registration error: {e}")
        raise HTTPException(status_code=400, detail=f"Registration failed: {e}")


@router.post("/login", response_model=TokenResponse)
def login(body: LoginRequest):
    """
    Authenticate an existing user.
    - AUTH_ENABLED=false: returns a mock dev token
    - AUTH_ENABLED=true: signs in via Supabase Auth
    """
    if not settings.AUTH_ENABLED:
        logger.info("[AUTH] Dev mode: returning mock login response")
        return TokenResponse(
            access_token=_DEV_TOKEN,
            user_id=DEV_USER["id"],
            email=body.email,
        )

    supabase = _get_supabase_client()
    try:
        response = supabase.auth.sign_in_with_password({
            "email": body.email,
            "password": body.password,
        })
        if not response.session:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        return TokenResponse(
            access_token=response.session.access_token,
            user_id=str(response.user.id) if response.user else None,
            email=response.user.email if response.user else None,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[AUTH] Login error: {e}")
        raise HTTPException(status_code=401, detail="Invalid credentials")


@router.get("/me", response_model=UserResponse)
def get_me(current_user: dict = Depends(get_current_user)):
    """Return the currently authenticated user's info."""
    return UserResponse(
        id=str(current_user.get("sub") or current_user.get("id", "")),
        email=current_user.get("email", ""),
        role=current_user.get("role", "authenticated"),
    )
