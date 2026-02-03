#!/usr/bin/env python3
"""
Simple startup script for the Databricks Genie Slack Bot.
This avoids potential module import issues.
"""

import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import and run the main function from src module
from src.utils import main

if __name__ == "__main__":
    main()