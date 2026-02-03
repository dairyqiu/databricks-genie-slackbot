"""
Databricks Genie Slack Bot source package.
"""

# Package metadata
__version__ = "1.0.0"
__author__ = "Databricks Genie Slackbot Team"

# Import key components for easier access
from .utils import main, bot_state
from .config import Config, load_configuration, validate_environment
from .databricks_client import get_databricks_client
from .slack_handlers import setup_slack_handlers, start_socket_mode

__all__ = [
    'main',
    'bot_state',
    'Config',
    'load_configuration',
    'validate_environment',
    'get_databricks_client',
    'setup_slack_handlers',
    'start_socket_mode'
]
