"""
Configuration module for Databricks Genie Slack Bot.

This module handles all configuration loading, validation, and constants.
"""

import os
import logging
from typing import Optional
from pathlib import Path

# Configure logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    # Look for .env file in current directory and parent directories
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Loaded .env file from {env_path}")
    else:
        # Try current directory
        load_dotenv()
        logger.info("Loaded .env file from current directory")
except ImportError:
    logger.warning("python-dotenv not installed, .env file will not be loaded automatically")


class Config:
    """Centralized Configuration Constants."""
    
    # Core application limits
    MAX_CONVERSATION_AGE = 3600  # 1 hour
    SLACK_FILE_SIZE_LIMIT = 50 * 1024 * 1024  # 50MB
    
    # Databricks client polling settings
    POLL_INTERVAL = 3  # seconds between polls
    MAX_WAIT_TIME = 120  # maximum wait time for operations
    
    # Thread pool configuration
    MAX_WORKER_THREADS = 30
    WORKER_THREAD_MULTIPLIER = 5
    MIN_CPU_COUNT = 2
    
    # System timing intervals
    HEARTBEAT_INTERVAL = 60  # seconds
    SOCKET_CONNECTION_DELAY = 2  # seconds
    MESSAGE_PROCESSING_DELAY = 0.1  # seconds
    PERFORMANCE_UPDATE_INTERVAL = 60  # seconds
    
    # Bot response patterns to prevent loops
    BOT_RESPONSE_PATTERNS = [
        "Genie:", "✅", "❌", "📁", "🔗", "📊", "⏱️", "📋", 
        "💾", "📦", "🆔", "⚠️", "Query generated but could not be executed automatically"
    ]
    
    
    # Databricks Genie API limits (aligned with documentation: 5 QPM for API usage)
    GENIE_MAX_CONCURRENT_CONVERSATIONS = 10  # Per workspace limit
    GENIE_RATE_LIMIT_PER_MINUTE = 5  # 5 QPM per workspace (API usage, as documented)
    GENIE_BURST_CAPACITY = 0  # No burst capacity - strict 5 QPM limit to match Genie API
    GENIE_MESSAGE_TIMEOUT = 600  # 10 minutes for long-running queries
    GENIE_POLL_INTERVAL = 7  # Poll every 7 seconds (between 5-10s as recommended)
    GENIE_BACKOFF_THRESHOLD = 120  # Start exponential backoff after 2 minutes
    
    # Enhanced throttling system configuration
    GENIE_QUEUE_MAX_SIZE = 50  # Maximum number of queued requests per workspace
    GENIE_QUEUE_TIMEOUT = 300  # 5 minutes max wait time in queue
    GENIE_RATE_LIMIT_WINDOW_SIZE = 60  # Sliding window size in seconds
    GENIE_THROTTLE_CHECK_INTERVAL = 1  # Check rate limits every second
    
    # No user-based concurrency limits - only workspace QPM limits apply


class ConfigurationError(Exception):
    """Raised when configuration is invalid."""
    pass


class GenieError(Exception):
    """Raised when Genie operations fail."""
    pass


class GenieRateLimitExceeded(GenieError):
    """Raised when Genie API returns rate limiting errors."""
    pass


def load_secret(scope: str, key: str, dbutils=None, workspace_client=None) -> Optional[str]:
    """Load a secret from Databricks secrets."""
    try:
        if dbutils is not None:
            return dbutils.secrets.get(scope=scope, key=key)
        elif workspace_client is not None:
            secret_response = workspace_client.secrets.get_secret(scope=scope, key=key)
            return secret_response.value if hasattr(secret_response, 'value') else str(secret_response)
    except Exception as e:
        logger.warning(f"Could not load secret {key} from scope {scope}: {e}")
    return None


def load_configuration(bot_state) -> None:
    """Load configuration from environment variables and secrets."""
    from .databricks_client import get_databricks_client, initialize_dbutils
    
    # Check for Databricks Apps environment
    app_databricks_host = os.getenv('DATABRICKS_HOST')
    app_client_id = os.getenv('DATABRICKS_CLIENT_ID')
    app_client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
    app_access_token = os.getenv('DATABRICKS_ACCESS_TOKEN')
    
    # Check if running locally (has DATABRICKS_ACCESS_TOKEN in env)
    is_local_deployment = app_access_token is not None
    logger.info(f"Local deployment detected: {is_local_deployment}")
    logger.info(f"Databricks Apps environment detected: {app_databricks_host is not None}")
    
    # Check authentication method
    has_oauth2_credentials = app_client_id and app_client_secret
    has_access_token = app_access_token is not None
    
    if not has_oauth2_credentials and not has_access_token:
        raise ConfigurationError(
            "Either DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET (OAuth2) "
            "or DATABRICKS_ACCESS_TOKEN (Personal Access Token) is required"
        )
    
    if has_oauth2_credentials and has_access_token:
        logger.warning("Both OAuth2 credentials and Personal Access Token found. Using OAuth2 credentials.")
    
    # Set authentication fields in bot_state
    bot_state.client_id = app_client_id
    bot_state.client_secret = app_client_secret
    bot_state.access_token = app_access_token
    
    # Initialize workspace client
    if app_databricks_host:
        bot_state.databricks_host = app_databricks_host
        
        try:
            # Create workspace client to test and configure
            bot_state.workspace_client = get_databricks_client(bot_state)
            auth_method = "OAuth2 service principal" if has_oauth2_credentials else "Personal Access Token"
            logger.info(f"Successfully authenticated using {auth_method}")
            
            # Initialize dbutils only if not running locally
            if not is_local_deployment:
                initialize_dbutils(bot_state.workspace_client, bot_state)
            
        except Exception as e:
            logger.error(f"Failed to initialize workspace client: {e}")
            raise
    
    # Load other configuration
    secret_scope = os.getenv('SECRET_SCOPE', 'slackbot-genie')
    bot_state.genie_space_id = os.getenv('GENIE_SPACE_ID')
    
    # Load Slack tokens - use environment variables if running locally, otherwise use Databricks secrets
    if is_local_deployment:
        # Use environment variables for local deployment
        bot_state.slack_app_token = os.getenv('SLACK_APP_TOKEN')
        bot_state.slack_bot_token = os.getenv('SLACK_BOT_TOKEN')
        logger.info("Using environment variables for Slack tokens (local deployment)")
    else:
        # Use Databricks secrets for Databricks Apps deployment
        bot_state.slack_app_token = load_secret(secret_scope, "SLACK_APP_TOKEN", bot_state.dbutils, bot_state.workspace_client) or os.getenv('SLACK_APP_TOKEN')
        bot_state.slack_bot_token = load_secret(secret_scope, "SLACK_BOT_TOKEN", bot_state.dbutils, bot_state.workspace_client) or os.getenv('SLACK_BOT_TOKEN')
        logger.info("Using Databricks secrets for Slack tokens (Databricks Apps deployment)")
    
    # Load SQL query display flag
    show_query_env = os.getenv('SHOW_SQL_QUERY', 'true')
    bot_state.show_sql_query = show_query_env.lower() in ('true', '1', 'yes', 'on')
    
    logger.info(f"Configuration loaded - SHOW_SQL_QUERY: {bot_state.show_sql_query}")


def validate_environment(bot_state) -> None:
    """Validate that all required configuration values are set."""
    required_vars = {
        'DATABRICKS_HOST': bot_state.databricks_host,
        'GENIE_SPACE_ID': bot_state.genie_space_id,
        'SLACK_APP_TOKEN': bot_state.slack_app_token,
        'SLACK_BOT_TOKEN': bot_state.slack_bot_token
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    
    if missing_vars:
        raise ConfigurationError(f"Missing required configuration values: {', '.join(missing_vars)}")
    
    logger.info("All required configuration values are set")