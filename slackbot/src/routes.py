"""
Flask routes module for Databricks Genie Slack Bot.

This module contains all Flask routes and API endpoints for the web application.
"""

import time
import logging
from functools import wraps
from flask import Flask, request, jsonify

from .config import Config
from .utils import (
    speak_with_genie_optimized
)

logger = logging.getLogger(__name__)


def require_api_key(f):
    """Decorator to require API key authentication for endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        from .utils import bot_state
        # If no API key is configured, allow access (for backward compatibility)
        if not bot_state.api_key:
            return f(*args, **kwargs)

        api_key = request.headers.get('X-API-Key')
        if not api_key or api_key != bot_state.api_key:
            logger.warning(f"Unauthorized API access attempt from {request.remote_addr}")
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated

# Flask app
app = Flask(__name__)


@app.route('/')
def health_check():
    """Health check endpoint with performance metrics."""
    from .utils import bot_state
    performance_metrics = bot_state.performance_monitor.get_metrics()
    
    return jsonify({
        'status': 'healthy',
        'service': 'slackbot-backend',
        'databricks_connected': bot_state.workspace_client is not None,
        'slack_connected': bot_state.web_client is not None,
        'socket_mode_active': bot_state.socket_mode_client is not None,
        'performance': {
            'metrics': performance_metrics
        }
    })


@app.route('/api/chat', methods=['POST'])
@require_api_key
def chat_endpoint():
    """Direct API endpoint for chat functionality."""
    from .utils import bot_state
    start_time = time.time()
    
    try:
        data = request.get_json()
        if not data or 'message' not in data:
            return jsonify({'error': 'Message is required'}), 400
        
        message = data['message']
        
        if not bot_state.workspace_client:
            return jsonify({'error': 'Databricks client not initialized'}), 500
        
        # Get response from Genie (API endpoint always starts fresh conversations) - using optimized version
        genie_response, csv_bytes, filename, conversation_id, message_id = speak_with_genie_optimized(message, bot_state.workspace_client, None, bot_state)
        
        response_data = {
            'response': genie_response,
            'has_csv': csv_bytes is not None,
            'filename': filename
        }
        
        # If CSV data is available and requested, include it
        if csv_bytes and data.get('include_csv', False):
            import base64
            response_data['csv_data'] = base64.b64encode(csv_bytes).decode('utf-8')
        
        # Record performance metrics
        response_time = time.time() - start_time
        bot_state.performance_monitor.record_query(response_time, success=True)
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        response_time = time.time() - start_time
        bot_state.performance_monitor.record_query(response_time, success=False)
        return jsonify({'error': str(e)}), 500


@app.route('/api/status')
@require_api_key
def status():
    """Get detailed status of the service with performance metrics."""
    from .utils import bot_state
    
    # Calculate user-based queue statistics
    total_queued_messages = sum(len(queue) for queue in bot_state.message_queue.values())
    active_user_queues = len([q for q in bot_state.message_queue.values() if q])
    
    # Get performance metrics
    performance_metrics = bot_state.performance_monitor.get_metrics()
    
    return jsonify({
        'status': 'running',
        'service': 'slackbot-backend',
        'databricks_host': bot_state.databricks_host,
        'genie_space_id': bot_state.genie_space_id,
        'databricks_connected': bot_state.workspace_client is not None,
        'slack_connected': bot_state.web_client is not None,
        'socket_mode_active': bot_state.socket_mode_client is not None,
        'socket_thread_running': bot_state.socket_thread is not None and bot_state.socket_thread.is_alive(),
        'conversation_stats': {
            'active_conversations': len(bot_state.conversation_tracker),
            'max_conversation_age_seconds': Config.MAX_CONVERSATION_AGE
        },
        'queue_stats': {
            'processing_users': len(bot_state.processing_users),
            'active_user_queues': active_user_queues,
            'total_queued_messages': total_queued_messages,
            'note': 'No rate limiting - requests processed immediately',
            'user_queue_details': {
                user_id: len(queue) for user_id, queue in bot_state.message_queue.items() if queue
            }
        },
        'performance': {
            'metrics': performance_metrics
        }
    })


@app.route('/api/performance')
@require_api_key
def performance_metrics():
    """Get detailed performance metrics."""
    from .utils import bot_state
    performance_metrics = bot_state.performance_monitor.get_metrics()

    return jsonify({
        'system_metrics': performance_metrics,
        'optimizations': {
            'memory_monitoring': True,
            'buffer_optimization': True
        }
    })


@app.route('/api/diagnostics')
@require_api_key
def diagnostics():
    """Check service principal permissions for Genie."""
    import os
    from .utils import bot_state
    from .databricks_client import verify_genie_permissions

    results = verify_genie_permissions(
        bot_state.workspace_client,
        bot_state.genie_space_id
    )

    return jsonify({
        'service_principal_client_id': os.getenv('DATABRICKS_CLIENT_ID'),
        'genie_space_id': bot_state.genie_space_id,
        'databricks_host': bot_state.databricks_host,
        'permissions': results
    }) 