import os

from dotenv import load_dotenv

load_dotenv()


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        raise RuntimeError(
            f"Missing required configuration: {name}. "
            "Set it in the process environment or .env."
        )
    return value


def get_google_api_key() -> str:
    return get_required_env("GOOGLE_API_KEY")


def get_tavily_api_key() -> str:
    return get_required_env("TAVILY_API_KEY")


def get_cloudflare_credentials() -> tuple[str, str]:
    return (
        get_required_env("CLOUDFLARE_ACCOUNT_ID"),
        get_required_env("CLOUDFLARE_API_TOKEN"),
    )


def get_supabase_db_uri() -> str:
    return get_required_env("SUPABASE_DB_URI")
