"""Slack client for fetching messages and downloading PDF attachments."""
import logging
from pathlib import Path
from typing import List, Dict, Any
import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from config import Config

logger = logging.getLogger(__name__)


class SlackClient:
    """Client for interacting with Slack API."""
    
    def __init__(self, token: str):
        """Initialize Slack client with bot token.
        
        Args:
            token: Slack bot token (xoxb-...)
        """
        self.client = WebClient(token=token)
        self.token = token
    
    def get_channel_id(self, channel_name: str) -> str:
        """Get channel ID from channel name.
        
        Args:
            channel_name: Name of the channel (without #)
            
        Returns:
            Channel ID
            
        Raises:
            ValueError: If channel not found
        """
        try:
            # Try to use the channel name directly first
            response = self.client.conversations_list()
            for channel in response['channels']:
                if channel['name'] == channel_name:
                    return channel['id']
            
            raise ValueError(f"Channel '{channel_name}' not found")
        except SlackApiError as e:
            logger.error(f"Error fetching channel ID: {e}")
            raise
    
    def fetch_messages(self, channel: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch message history from a channel.
        
        Args:
            channel: Channel ID or name
            limit: Maximum number of messages to fetch per page
            
        Returns:
            List of message objects with file attachments
        """
        # Get channel ID if channel name is provided
        if not channel.startswith('C'):
            channel_id = self.get_channel_id(channel)
        else:
            channel_id = channel
        
        messages_with_pdfs = []
        cursor = None
        
        try:
            while True:
                logger.info(f"Fetching messages from channel {channel_id}...")
                response = self.client.conversations_history(
                    channel=channel_id,
                    limit=limit,
                    cursor=cursor
                )
                
                # Filter messages that have PDF file attachments
                for message in response['messages']:
                    if 'files' in message:
                        pdf_files = [f for f in message['files'] if f.get('mimetype') == 'application/pdf']
                        if pdf_files:
                            message['pdf_files'] = pdf_files
                            messages_with_pdfs.append(message)
                            logger.info(f"Found message with {len(pdf_files)} PDF(s)")
                
                # Check if there are more pages
                if not response.get('has_more', False):
                    break
                    
                cursor = response.get('response_metadata', {}).get('next_cursor')
                if not cursor:
                    break
            
            logger.info(f"Found {len(messages_with_pdfs)} messages with PDF attachments")
            return messages_with_pdfs
            
        except SlackApiError as e:
            logger.error(f"Error fetching messages: {e}")
            raise
    
    def download_pdf(self, file_info: Dict[str, Any], download_path: Path) -> Path:
        """Download a PDF file from Slack.
        
        Args:
            file_info: File information dictionary from Slack API
            download_path: Directory to save the file
            
        Returns:
            Path to the downloaded file
        """
        file_id = file_info['id']
        file_name = file_info.get('name', f"{file_id}.pdf")
        url_private = file_info['url_private']
        
        # Create safe filename
        safe_filename = f"{file_id}_{file_name}"
        file_path = download_path / safe_filename
        
        # Skip if already downloaded (idempotency)
        if file_path.exists():
            logger.info(f"File {safe_filename} already exists, skipping download")
            return file_path
        
        try:
            logger.info(f"Downloading {safe_filename}...")
            headers = {'Authorization': f'Bearer {self.token}'}
            response = requests.get(url_private, headers=headers, stream=True)
            response.raise_for_status()
            
            # Ensure directory exists
            download_path.mkdir(parents=True, exist_ok=True)
            
            # Save file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logger.info(f"Downloaded: {file_path}")
            return file_path
            
        except Exception as e:
            logger.error(f"Error downloading file {file_name}: {e}")
            raise
    
    def download_all_pdfs(self, messages: List[Dict[str, Any]], download_path: Path) -> List[Dict[str, Any]]:
        """Download all PDF files from messages.
        
        Args:
            messages: List of messages with PDF files
            download_path: Directory to save files
            
        Returns:
            List of dictionaries with file metadata and local paths
        """
        downloaded_files = []
        
        for message in messages:
            message_ts = message.get('ts', '')
            message_text = message.get('text', '')
            
            for file_info in message.get('pdf_files', []):
                try:
                    local_path = self.download_pdf(file_info, download_path)
                    
                    downloaded_files.append({
                        'file_id': file_info['id'],
                        'file_name': file_info.get('name', 'unknown.pdf'),
                        'local_path': local_path,
                        'slack_url': file_info.get('permalink', ''),
                        'message_ts': message_ts,
                        'message_text': message_text,
                        'file_size': file_info.get('size', 0)
                    })
                except Exception as e:
                    logger.error(f"Failed to download file: {e}")
                    continue
        
        logger.info(f"Downloaded {len(downloaded_files)} PDF files")
        return downloaded_files

