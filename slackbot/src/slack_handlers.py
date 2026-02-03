"""
Slack handlers module for Databricks Genie Slack Bot.

This module handles all Slack-specific logic including event handlers, Socket Mode setup,
and message processing.
"""

import time
import logging
import threading
from typing import Dict, Any, Optional

import slack_sdk
from slack_sdk.web import WebClient
from slack_sdk.socket_mode import SocketModeClient
from slack_sdk.socket_mode.response import SocketModeResponse
from slack_sdk.socket_mode.request import SocketModeRequest

from .config import Config

logger = logging.getLogger(__name__)


def get_slack_web_client(bot_state) -> WebClient:
    """Initialize and return a secure Slack WebClient."""
    try:
        # Create secure Slack client (SSL verification enabled by default)
        client = WebClient(token=bot_state.slack_bot_token)
        auth_test = client.auth_test()
        
        logger.info(f"Securely connected to Slack workspace: {auth_test['team']}")
        logger.info(f"Bot user: {auth_test['user']}")
        
        # Store bot user ID for loop prevention
        bot_state.bot_user_id = auth_test.get('user_id')
        logger.info(f"Bot user ID: {bot_state.bot_user_id}")
        
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Slack WebClient: {e}")
        raise


def get_socket_mode_client(bot_state) -> SocketModeClient:
    """Initialize and return a Socket Mode client following SDK best practices."""
    try:
        if not bot_state.slack_app_token:
            raise ValueError("SLACK_APP_TOKEN is required for Socket Mode")
        
        if not bot_state.slack_app_token.startswith('xapp-'):
            raise ValueError("SLACK_APP_TOKEN should start with 'xapp-' (Socket Mode token)")
        
        # Create WebClient for API calls
        web_client = get_slack_web_client(bot_state)
        
        logger.info(f"Creating Socket Mode client with app token: {bot_state.slack_app_token[:10]}...")
        client = SocketModeClient(
            app_token=bot_state.slack_app_token,
            web_client=web_client,
            auto_reconnect_enabled=True,  # SDK handles reconnection
            trace_enabled=False  # Enable only for debugging
        )
        
        # Register the proper request listener
        client.socket_mode_request_listeners.append(
            lambda client, req: process_socket_mode_request(client, req, bot_state)
        )
        
        logger.info("Successfully initialized Socket Mode client")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Socket Mode client: {e}")
        raise


def process_socket_mode_request(client: SocketModeClient, req: SocketModeRequest, bot_state):
    """Process Socket Mode requests with proper acknowledgment following SDK best practices."""
    try:
        # CRITICAL: Always acknowledge first to prevent message loss
        response = SocketModeResponse(envelope_id=req.envelope_id)
        client.send_socket_mode_response(response)

        if req.type == "events_api":
            event = req.payload["event"]
            event_type = event.get("type")
            logger.info(f"📥 Received event type: {event_type}")
            logger.debug(f"Event details: {event}")

            # Handle reaction events first (before bot message filtering)
            if event_type == "reaction_added":
                logger.info(f"🎯 Received reaction_added event: {event.get('reaction')} from user {event.get('user')}")
                handle_reaction_event(event, client.web_client, bot_state)
                return

            # Skip bot messages to prevent loops (only for message events)
            if event.get('bot_id') or event.get('subtype') in ['bot_message', 'me_message']:
                logger.debug("Ignoring bot message to prevent loops")
                return

            # Skip messages from the bot itself
            user_id = event.get("user", "")
            if bot_state.bot_user_id and user_id == bot_state.bot_user_id:
                logger.debug("Ignoring message from bot itself")
                return

            # Handle different event types
            if event_type == "message":
                handle_message_event(event, client.web_client, bot_state)
            elif event_type == "app_mention":
                handle_app_mention_event(event, client.web_client, bot_state)

    except Exception as e:
        logger.error(f"Error processing Socket Mode request: {e}")


def handle_message_event(event: Dict[str, Any], web_client: WebClient, bot_state):
    """Handle message events - only process DMs."""
    channel_id = event.get("channel", "")
    
    # Only process direct messages (DMs)
    if channel_id.startswith("D"):
        logger.debug(f"Processing direct message in channel {channel_id}")
        from .utils import add_message_to_queue
        add_message_to_queue(event.get("user", "unknown"), event, 
                           lambda text, thread_ts=None: web_client.chat_postMessage(
                               channel=channel_id, text=text, thread_ts=thread_ts), 
                           web_client, bot_state)
    else:
        logger.debug(f"Ignoring channel message in {channel_id} - requires explicit mention (@bot)")


def handle_app_mention_event(event: Dict[str, Any], web_client: WebClient, bot_state):
    """Handle app mention events - process mentions in channels and threads."""
    channel_id = event.get("channel", "unknown")
    user_id = event.get("user", "unknown")

    logger.info(f"📨 Received app_mention event: channel: {channel_id}, user: {user_id}")

    from .utils import add_message_to_queue
    add_message_to_queue(user_id, event,
                       lambda text, thread_ts=None: web_client.chat_postMessage(
                           channel=channel_id, text=text, thread_ts=thread_ts),
                       web_client, bot_state)


def handle_reaction_event(event: Dict[str, Any], web_client: WebClient, bot_state):
    """Handle reaction events for collecting user feedback on Genie responses."""
    reaction = event.get("reaction", "")
    item = event.get("item", {})
    item_ts = item.get("ts", "")
    channel = item.get("channel", "")
    user_id = event.get("user", "unknown")

    # Only process thumbs up/down reactions
    positive_reactions = ["+1", "thumbsup", "white_check_mark", "heavy_check_mark"]
    negative_reactions = ["-1", "thumbsdown", "x", "heavy_multiplication_x"]

    is_positive = reaction in positive_reactions
    is_negative = reaction in negative_reactions

    if not is_positive and not is_negative:
        logger.debug(f"Ignoring non-feedback reaction: {reaction}")
        return

    logger.info(f"👍 Received feedback reaction '{reaction}' from user {user_id} on message {item_ts}")

    # Look up the Genie message ID for this Slack message
    from threading import RLock
    with bot_state.message_id_lock:
        message_info = bot_state.message_id_tracker.get(item_ts)

    if not message_info:
        logger.debug(f"No Genie message mapping found for Slack message {item_ts}")
        return

    conversation_id = message_info.get("conversation_id")
    message_id = message_info.get("message_id")

    if not conversation_id or not message_id:
        logger.warning(f"Incomplete message info for {item_ts}: {message_info}")
        return

    # Send feedback to Genie
    try:
        from .databricks_client import send_genie_feedback
        success = send_genie_feedback(
            workspace_client=bot_state.workspace_client,
            space_id=bot_state.genie_space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            is_positive=is_positive
        )

        if success:
            feedback_type = "positive" if is_positive else "negative"
            logger.info(f"✅ Successfully sent {feedback_type} feedback for message {message_id}")

            # Send confirmation to user by reacting to the original message with 👀
            try:
                web_client.reactions_add(
                    channel=channel,
                    timestamp=item_ts,
                    name="eyes"
                )
            except Exception as react_err:
                # Don't fail if we can't add reaction (e.g., already reacted)
                logger.debug(f"Could not add confirmation reaction: {react_err}")
    except Exception as e:
        logger.error(f"Failed to send feedback for message {message_id}: {e}")


def get_channel_info(client, channel_id: str) -> Optional[Dict[str, Any]]:
    """Get channel information to determine channel type and permissions."""
    try:
        # Try to get channel info
        response = client.conversations_info(channel=channel_id)
        if response.get('ok'):
            return response.get('channel', {})
    except Exception as e:
        logger.debug(f"Could not get channel info for {channel_id}: {e}")
    
    return None


def can_bot_post_to_channel(client, channel_id: str) -> bool:
    """Check if the bot can post to a specific channel."""
    # Direct messages (DMs) always allow the bot to post
    if channel_id.startswith("D"):
        logger.debug(f"Channel {channel_id} is a DM - bot can always post")
        return True
    
    try:
        # Get channel info
        channel_info = get_channel_info(client, channel_id)
        if not channel_info:
            return False
        
        # Check if bot is a member of the channel
        is_member = channel_info.get('is_member', False)
        if not is_member:
            logger.debug(f"Bot is not a member of channel {channel_id}")
            return False
        
        # Check channel type
        channel_type = channel_info.get('is_private', False)
        if channel_type:
            logger.debug(f"Channel {channel_id} is private - bot must be explicitly invited")
        else:
            logger.debug(f"Channel {channel_id} is public")
        
        return True
        
    except Exception as e:
        logger.error(f"Error checking bot permissions for channel {channel_id}: {e}")
        return False


def invite_bot_to_channel(client, channel_id: str) -> bool:
    """Attempt to invite the bot to a channel."""
    # Cannot invite bot to DM channels
    if channel_id.startswith("D"):
        logger.debug(f"Cannot invite bot to DM channel {channel_id}")
        return False
    
    try:
        # Get bot user ID
        auth_test = client.auth_test()
        bot_user_id = auth_test.get('user_id')
        
        if not bot_user_id:
            logger.error("Could not get bot user ID")
            return False
        
        # Try to invite bot to channel
        response = client.conversations_invite(
            channel=channel_id,
            users=[bot_user_id]
        )
        
        if response.get('ok'):
            logger.info(f"Successfully invited bot to channel {channel_id}")
            return True
        else:
            error = response.get('error', 'unknown error')
            logger.warning(f"Failed to invite bot to channel {channel_id}: {error}")
            return False
            
    except Exception as e:
        logger.error(f"Error inviting bot to channel {channel_id}: {e}")
        return False


def setup_slack_handlers(bot_state) -> None:
    """Set up Slack Socket Mode client - handlers are now in process_socket_mode_request."""
    logger.info("✅ Slack Socket Mode client configured with proper event acknowledgment")
    logger.info("📡 Event handling: message (DM only), app_mention (channels/threads) with SDK best practices")


def start_socket_mode(bot_state) -> None:
    """Start the Socket Mode client using SDK best practices."""
    if bot_state.socket_mode_client is None:
        logger.error("Socket Mode client not initialized")
        return
    
    def run_socket_client() -> None:
        try:
            logger.info("Starting Socket Mode client...")
            # Connect using SDK's built-in connection management
            bot_state.socket_mode_client.connect()
        except Exception as e:
            logger.error(f"Socket Mode client error: {e}")
            bot_state.socket_handler_error = e
    
    bot_state.socket_thread = threading.Thread(target=run_socket_client, daemon=True)
    bot_state.socket_thread.start()
    logger.info("Socket Mode client started in background thread")
    
    # Give it a moment to start and verify connection
    time.sleep(Config.SOCKET_CONNECTION_DELAY)
    
    # Check if thread is alive and client is running
    if bot_state.socket_thread.is_alive():
        logger.info("✅ Socket Mode thread is running")
        
        # Additional verification - check if client is connected
        time.sleep(2)  # Give more time for connection
        try:
            if bot_state.socket_mode_client.is_connected():
                logger.info("✅ Socket Mode client is connected")
            else:
                logger.warning("⚠️ Socket Mode client may not be properly connected")
        except AttributeError:
            logger.debug("Connection status check not available")
            
        # Check for any errors during startup
        if hasattr(bot_state, 'socket_handler_error') and bot_state.socket_handler_error:
            logger.error(f"❌ Socket client encountered error: {bot_state.socket_handler_error}")
    else:
        logger.error("❌ Socket Mode thread failed to start") 