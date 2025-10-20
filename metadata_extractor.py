"""Metadata extraction using OpenAI API."""
import logging
import json
import time
from typing import Dict, Optional, Any, List
from openai import OpenAI, RateLimitError, APIError

from config import Config

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """Extract document metadata using OpenAI API."""
    
    def __init__(self, api_key: str):
        """Initialize metadata extractor.
        
        Args:
            api_key: OpenAI API key
        """
        self.client = OpenAI(api_key=api_key)
        self.model = "gpt-4o-mini"  # Cost-effective model for metadata extraction
    
    def extract_metadata(self, text: str, max_retries: int = 3) -> Dict[str, Optional[str]]:
        """Extract document title and publication date from text using OpenAI.
        
        Args:
            text: Extracted text from PDF
            max_retries: Maximum number of retry attempts for rate limits
            
        Returns:
            Dictionary with 'title' and 'publication_date' keys
        """
        # Truncate text to first 3000 characters for cost efficiency
        # Most document metadata is in the first few pages
        truncated_text = text[:3000] if len(text) > 3000 else text
        
        # Design an effective prompt for metadata extraction
        system_prompt = """You are a document metadata extraction assistant. 
Your task is to extract the document title and publication/creation date from the provided text.

Return your response in JSON format with exactly these keys:
- "title": The main title of the document (string, or null if not found)
- "publication_date": The publication or creation date in ISO format YYYY-MM-DD (string, or null if not found)

If you cannot find either field, return null for that field.
Be precise and extract only what is clearly stated in the document."""

        user_prompt = f"""Extract the title and publication date from this document text:

{truncated_text}

Return JSON with "title" and "publication_date" fields."""

        for attempt in range(max_retries):
            try:
                logger.info(f"Calling OpenAI API for metadata extraction (attempt {attempt + 1}/{max_retries})...")
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.1,  # Low temperature for consistent extraction
                    max_tokens=200
                )
                
                result = json.loads(response.choices[0].message.content)
                
                logger.info(f"Extracted metadata: {result}")
                
                return {
                    'title': result.get('title'),
                    'publication_date': result.get('publication_date')
                }
                
            except RateLimitError as e:
                logger.warning(f"Rate limit hit: {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    logger.error("Max retries reached for rate limit")
                    return {'title': None, 'publication_date': None}
                    
            except APIError as e:
                logger.error(f"OpenAI API error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                else:
                    return {'title': None, 'publication_date': None}
                    
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse OpenAI response as JSON: {e}")
                return {'title': None, 'publication_date': None}
                
            except Exception as e:
                logger.error(f"Unexpected error during metadata extraction: {e}")
                return {'title': None, 'publication_date': None}
        
        return {'title': None, 'publication_date': None}
    
    def process_files(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process multiple files and extract metadata from each.
        
        Args:
            files: List of file dictionaries with extracted_text
            
        Returns:
            List of files with added metadata fields
        """
        logger.info(f"Extracting metadata from {len(files)} files...")
        
        processed_files = []
        for i, file_info in enumerate(files, 1):
            logger.info(f"Processing file {i}/{len(files)}: {file_info['file_name']}")
            
            text = file_info.get('extracted_text', '')
            if not text:
                logger.warning(f"No text available for {file_info['file_name']}")
                metadata = {'title': None, 'publication_date': None}
            else:
                metadata = self.extract_metadata(text)
            
            processed_files.append({
                **file_info,
                **metadata
            })
            
            # Small delay to avoid hitting rate limits
            if i < len(files):
                time.sleep(0.5)
        
        logger.info("Metadata extraction complete")
        return processed_files

