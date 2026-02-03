"""
Databricks client module for Genie Slack Bot.

This module handles Databricks WorkspaceClient creation and Genie API operations.
"""

import time
import logging
from typing import Optional, List, Dict, Any, Tuple
from databricks.sdk import WorkspaceClient
from .config import Config, GenieError

logger = logging.getLogger(__name__)


def get_databricks_client_optimized(bot_state) -> WorkspaceClient:
    """Create Databricks client using SDK best practices with automatic credential detection."""
    try:
        # Use SDK's automatic credential detection when possible
        if bot_state.client_id and bot_state.client_secret:
            logger.info("Using OAuth2 client credentials authentication")
            client = WorkspaceClient(
                host=bot_state.databricks_host,
                client_id=bot_state.client_id,
                client_secret=bot_state.client_secret
            )
        elif bot_state.access_token:
            logger.info("Using personal access token authentication")
            client = WorkspaceClient(
                host=bot_state.databricks_host,
                token=bot_state.access_token
            )
        else:
            logger.info("Using SDK automatic credential detection")
            # Let SDK auto-detect credentials from environment
            client = WorkspaceClient(host=bot_state.databricks_host)
        
        # Test the connection
        current_user = client.current_user.me()
        logger.info(f"Successfully authenticated to Databricks workspace: {bot_state.databricks_host}")
        logger.info(f"Authenticated as user: {getattr(current_user, 'user_name', 'unknown')}")
        
        return client
        
    except Exception as e:
        logger.error(f"Failed to create optimized Databricks client: {e}")
        raise


def get_databricks_client(bot_state) -> WorkspaceClient:
    """Main function - now uses optimized SDK implementation."""
    return get_databricks_client_optimized(bot_state)


def initialize_dbutils(workspace_client: WorkspaceClient, bot_state) -> None:
    """Initialize dbutils with the workspace client for secrets access."""
    try:
        if bot_state.dbutils is None and workspace_client is not None:
            from databricks.sdk.dbutils import RemoteDbUtils
            bot_state.dbutils = RemoteDbUtils(workspace_client._config)
            logger.info("Initialized dbutils with workspace client for secrets access")
    except Exception as e:
        logger.warning(f"Could not initialize dbutils: {e}")
        bot_state.dbutils = None


def wait_for_message_completion(workspace_client: WorkspaceClient, space_id: str, conversation_id: str, message_id: str, max_wait_time: int = Config.MAX_WAIT_TIME) -> Any:
    """Wait for message generation to complete and return the result."""
    start_time = time.time()
    poll_interval = Config.POLL_INTERVAL
    
    while time.time() - start_time < max_wait_time:
        try:
            message_result = workspace_client.genie.get_message(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id
            )
            
            if message_result is None:
                logger.debug("Received None message result, continuing to poll")
                time.sleep(poll_interval)
                continue
            
            # Get status value
            status_value = None
            if message_result.status is not None:
                status_value = message_result.status.value if hasattr(message_result.status, 'value') else str(message_result.status)
            else:
                # Try alternative status fields
                for attr in ['state', 'phase', 'stage']:
                    try:
                        alt_status = getattr(message_result, attr, None)
                        if alt_status is not None:
                            status_value = alt_status.value if hasattr(alt_status, 'value') else str(alt_status)
                            logger.debug(f"Using alternative status field '{attr}': {status_value}")
                            break
                    except (AttributeError, Exception):
                        continue
            
            if status_value is None:
                logger.debug("No status found in any field, treating as IN_PROGRESS and continuing to poll")
                time.sleep(poll_interval)
                continue
            
            logger.debug(f"Processing message status: {status_value}")
            
            if status_value == 'COMPLETED':
                logger.info(f"Message generation completed successfully with status: {status_value}")
                return message_result
            elif status_value == 'FAILED':
                error_msg = str(message_result.error) if message_result.error else "Unknown error"
                raise GenieError(f"Message generation failed: {error_msg}")
            elif status_value == 'CANCELLED':
                raise GenieError("Message generation was cancelled")
            elif status_value in ['IN_PROGRESS', 'EXECUTING_QUERY']:
                logger.debug(f"Message still processing, status: {status_value}")

                # Apply exponential backoff after 2 minutes
                if time.time() - start_time > 120:
                    poll_interval = min(poll_interval * 1.5, 30)
                
                time.sleep(poll_interval)
                continue
            else:
                logger.debug(f"Unknown status: {status_value}, continuing to poll")
                time.sleep(poll_interval)
                continue
            
        except Exception as e:
            logger.error(f"Error checking message status: {e}")
            
            # Check for specific error types
            if "PermissionDenied" in str(e) or "You need" in str(e):
                raise GenieError(f"Conversation access denied: {e}")
            elif "timeout" in str(e).lower() or "connection" in str(e).lower():
                raise GenieError(f"Network error during message polling: {e}")
            
            continue
    
    raise GenieError(f"Message generation timed out after {max_wait_time} seconds")


def wait_for_query_completion_if_needed(query_result: Any, workspace_client: WorkspaceClient, max_wait_time: int = 60) -> Any:
    """Check if query result needs to wait for completion and wait if necessary."""
    try:
        statement_response = getattr(query_result, 'statement_response', None)
        if statement_response:
            status = getattr(statement_response, 'status', None)
            if status:
                state = getattr(status, 'state', None)
                if state:
                    state_value = state.value if hasattr(state, 'value') else str(state)
                    logger.debug(f"Query result state: {state_value}")
                    
                    if state_value in ['PENDING', 'RUNNING', 'EXECUTING']:
                        logger.info("Query is still executing, waiting for completion...")
                        return wait_for_query_completion(query_result, workspace_client, max_wait_time)
                    elif state_value in ['FINISHED', 'SUCCEEDED', 'COMPLETED']:
                        logger.info("Query is already completed")
                        return query_result
                    elif state_value in ['FAILED', 'CANCELLED', 'CLOSED']:
                        error_msg = str(status.error) if status.error else "Unknown error"
                        raise GenieError(f"Query execution failed: {error_msg}")
        
        logger.debug("Query result appears to be already complete or from a different source")
        return query_result
        
    except Exception as e:
        logger.error(f"Error checking query completion status: {e}")
        return query_result


def wait_for_query_completion(query_result: Any, workspace_client: WorkspaceClient, max_wait_time: int = 60) -> Any:
    """Wait for query execution to complete and return the final result."""
    start_time = time.time()
    poll_interval = 5
    
    # Extract statement ID
    statement_id = None
    try:
        statement_response = getattr(query_result, 'statement_response', None)
        if statement_response:
            statement_id = getattr(statement_response, 'statement_id', None)
    except (KeyError, AttributeError):
        pass
    
    if not statement_id:
        logger.warning("No statement ID found in query result, cannot wait for completion")
        return query_result
    
    logger.info(f"Waiting for query completion with statement ID: {statement_id}")
    
    while time.time() - start_time < max_wait_time:
        try:
            statement_status = workspace_client.statement_execution.get_statement(statement_id)
            
            if statement_status is None:
                logger.debug("Received None statement status, continuing to poll")
                time.sleep(poll_interval)
                continue
            
            state = getattr(statement_status, 'status', None)
            if state:
                state_value = state.state.value if hasattr(state.state, 'value') else str(state.state)
                logger.debug(f"Statement state: {state_value}")
                
                if state_value in ['FINISHED', 'SUCCEEDED', 'COMPLETED']:
                    logger.info(f"Query execution completed successfully with state: {state_value}")
                    return statement_status
                elif state_value in ['FAILED', 'CANCELLED', 'CLOSED']:
                    error_msg = str(state.error) if state.error else "Unknown error"
                    raise GenieError(f"Query execution failed: {error_msg}")
                elif state_value in ['PENDING', 'RUNNING', 'EXECUTING']:
                    logger.debug(f"Query still executing, state: {state_value}")
                    
                    # Apply exponential backoff after 2 minutes
                    if time.time() - start_time > 120:
                        poll_interval = min(poll_interval * 1.5, 30)
                    
                    time.sleep(poll_interval)
                    continue
                else:
                    logger.debug(f"Unknown state: {state_value}, continuing to poll")
                    time.sleep(poll_interval)
                    continue
            else:
                logger.debug("No state found in statement status, continuing to poll")
                time.sleep(poll_interval)
                continue
                
        except Exception as e:
            logger.error(f"Error checking statement status: {e}")
            time.sleep(poll_interval)
    
    raise GenieError(f"Query execution timed out after {max_wait_time} seconds")


def execute_query_with_fallback(workspace_client: WorkspaceClient, space_id: str, conversation_id: str, message_id: str, attachment_id: Optional[str], query_attachment: Any) -> Optional[Any]:
    """Execute query with multiple fallback strategies."""
    query_result = None
    
    # Try to get results by attachment ID first
    if attachment_id:
        logger.info("Getting query results using get_message_query_result_by_attachment...")
        try:
            query_result = workspace_client.genie.get_message_query_result_by_attachment(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id,
                attachment_id=attachment_id
            )
            return query_result
        except Exception as e:
            logger.warning(f"Failed to get results by attachment: {e}")
    
    # Try Genie's execute method using the preferred attachment-based API
    if attachment_id:
        logger.info("Attempting to execute query using execute_message_attachment_query...")
        try:
            query_result = workspace_client.genie.execute_message_attachment_query(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id,
                attachment_id=attachment_id
            )
            logger.info("Successfully executed query using attachment-based API")
            return query_result
        except Exception as attachment_exec_error:
            logger.warning(f"Attachment-based execute method failed: {attachment_exec_error}")
    
    # Fall back to direct warehouse execution
    logger.info("Falling back to direct warehouse execution...")
    try:
        warehouses = list(workspace_client.warehouses.list())
        if warehouses:
            warehouse_id = warehouses[0].id
            if warehouse_id:
                logger.info(f"Using warehouse ID: {warehouse_id}")
                
                query_result = workspace_client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id,
                    statement=query_attachment.query,
                    wait_timeout="900s"  # 15 minutes to match Genie's timeout
                )
                logger.info("Successfully executed query directly")
                return query_result
            else:
                logger.warning("Warehouse ID is None, cannot execute query directly")
        else:
            logger.warning("No warehouses available for direct query execution")
    except Exception as direct_exec_error:
        logger.error(f"Direct query execution failed: {direct_exec_error}")
        
        # Check for specific error types
        error_str = str(direct_exec_error).lower()
        if "permission" in error_str or "access" in error_str:
            logger.warning("Query failed due to permission issues - this is common with system tables")
        elif "timeout" in error_str or "interrupted" in error_str:
            logger.warning("Query failed due to timeout - system queries can take longer")
        elif "not found" in error_str or "does not exist" in error_str:
            logger.warning("Query failed because table or schema does not exist")
    
    return None


def extract_conversation_ids(conv_response: Any) -> Tuple[Optional[str], Optional[str]]:
    """Extract conversation and message IDs from response with simplified checking."""
    # Extract conversation ID
    conversation = safe_getattr(conv_response, 'conversation')
    conv_id = safe_getattr(conversation, 'id') if conversation else None
    if not conv_id:
        conv_id = safe_getattr(conv_response, 'conversation_id')
    
    # Extract message ID
    message = safe_getattr(conv_response, 'message')
    message_id = safe_getattr(message, 'id') if message else None
    if not message_id:
        message_id = safe_extract_id(conv_response, ['message_id', 'id'])
    
    return conv_id, message_id


def safe_getattr(obj: Any, attr: str, default: Any = None, log_errors: bool = False) -> Any:
    """Safely get an attribute from an object."""
    try:
        return getattr(obj, attr, default)
    except Exception as e:
        if log_errors:
            logger.error(f"Error getting attribute {attr}: {e}")
        return default


def safe_extract_id(response: Any, id_fields: List[str] = None) -> Optional[str]:
    """Safely extract ID from response using multiple possible field names."""
    if id_fields is None:
        id_fields = ['id', 'message_id', 'conversation_id']

    for field in id_fields:
        try:
            value = getattr(response, field, None)
            if value:
                return str(value)
        except Exception:
            continue

    return None


def send_genie_feedback(workspace_client: WorkspaceClient, space_id: str,
                       conversation_id: str, message_id: str, is_positive: bool) -> bool:
    """Send user feedback to Genie for a specific message."""
    try:
        from databricks.sdk.service.dashboards import FeedbackRating
        rating = FeedbackRating.POSITIVE if is_positive else FeedbackRating.NEGATIVE

        logger.info(f"📤 Sending feedback to Genie API: space_id={space_id}, conversation_id={conversation_id}, message_id={message_id}, rating={rating}")
        workspace_client.genie.send_message_feedback(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            rating=rating
        )
        logger.info(f"✅ Successfully sent {'positive' if is_positive else 'negative'} feedback for message {message_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send feedback for message {message_id}: {e}")
        return False


def verify_genie_permissions(workspace_client: WorkspaceClient, space_id: str) -> Dict[str, Any]:
    """Verify service principal has required permissions for Genie."""
    results = {
        'authenticated_user': None,
        'genie_space_accessible': False,
        'genie_space_details': None,
        'warehouses_accessible': False,
        'warehouse_count': 0,
        'errors': []
    }

    # 1. Check authenticated identity
    try:
        current_user = workspace_client.current_user.me()
        results['authenticated_user'] = {
            'user_name': getattr(current_user, 'user_name', None),
            'display_name': getattr(current_user, 'display_name', None),
            'id': getattr(current_user, 'id', None),
            'active': getattr(current_user, 'active', None)
        }
    except Exception as e:
        results['errors'].append(f"Failed to get current user: {e}")

    # 2. Check Genie space access
    try:
        space = workspace_client.genie.get_space(space_id=space_id)
        results['genie_space_accessible'] = True
        results['genie_space_details'] = {
            'id': getattr(space, 'id', None),
            'title': getattr(space, 'title', None)
        }
    except Exception as e:
        results['errors'].append(f"Failed to access Genie space {space_id}: {e}")

    # 3. Check warehouse access
    try:
        warehouses = list(workspace_client.warehouses.list())
        results['warehouses_accessible'] = True
        results['warehouse_count'] = len(warehouses)
    except Exception as e:
        results['errors'].append(f"Failed to list warehouses: {e}")

    return results


def get_large_result_download_url(workspace_client: WorkspaceClient, space_id: str,
                                 conversation_id: str, message_id: str,
                                 attachment_id: str, max_wait: int = 60) -> Optional[str]:
    """Generate download URL for large query results (>50MB)."""
    try:
        # Initiate the download generation
        workspace_client.genie.generate_download_full_query_result(
            space_id=space_id,
            conversation_id=conversation_id,
            message_id=message_id,
            attachment_id=attachment_id
        )

        # Poll for completion
        start = time.time()
        while time.time() - start < max_wait:
            try:
                result = workspace_client.genie.get_download_full_query_result(
                    space_id=space_id,
                    conversation_id=conversation_id,
                    message_id=message_id,
                    attachment_id=attachment_id
                )
                if hasattr(result, 'download_url') and result.download_url:
                    logger.info(f"Generated download URL for attachment {attachment_id}")
                    return result.download_url
            except Exception as poll_error:
                logger.debug(f"Polling for download URL: {poll_error}")
            time.sleep(2)

        logger.warning(f"Download generation timed out after {max_wait}s for attachment {attachment_id}")
        return None
    except Exception as e:
        logger.error(f"Failed to generate download URL for attachment {attachment_id}: {e}")
        return None