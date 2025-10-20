"""Main application entry point for document ingestion pipeline."""
import logging
import sys
import time
from pathlib import Path

from config import Config
from slack_client import SlackClient
from pdf_processor import PDFProcessor
from metadata_extractor import MetadataExtractor
from db_manager import DatabaseManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log')
    ]
)

logger = logging.getLogger(__name__)


def wait_for_database(db_manager: DatabaseManager, max_attempts: int = 30, delay: int = 2) -> bool:
    """Wait for database to be ready.
    
    Args:
        db_manager: Database manager instance
        max_attempts: Maximum number of connection attempts
        delay: Delay between attempts in seconds
        
    Returns:
        True if database is ready, False otherwise
    """
    logger.info("Waiting for database to be ready...")
    
    for attempt in range(max_attempts):
        try:
            with db_manager.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            logger.info("Database is ready!")
            return True
        except Exception as e:
            if attempt < max_attempts - 1:
                logger.info(f"Database not ready yet (attempt {attempt + 1}/{max_attempts}), waiting...")
                time.sleep(delay)
            else:
                logger.error(f"Failed to connect to database after {max_attempts} attempts: {e}")
                return False
    
    return False


def main():
    """Main pipeline execution."""
    logger.info("=" * 80)
    logger.info("Starting Document Ingestion Pipeline")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # Validate configuration
        Config.validate()
        logger.info("Configuration validated successfully")
        
        # Initialize components
        logger.info("Initializing components...")
        
        slack_client = SlackClient(Config.SLACK_BOT_TOKEN)
        pdf_processor = PDFProcessor(Config.MAX_WORKERS)
        metadata_extractor = MetadataExtractor(Config.OPENAI_API_KEY)
        db_manager = DatabaseManager(
            host=Config.POSTGRES_HOST,
            port=Config.POSTGRES_PORT,
            database=Config.POSTGRES_DB,
            user=Config.POSTGRES_USER,
            password=Config.POSTGRES_PASSWORD
        )
        
        # Wait for database and initialize schema
        if not wait_for_database(db_manager):
            logger.error("Database is not available. Exiting.")
            sys.exit(1)
        
        db_manager.init_schema()
        
        # Step 1: Fetch messages from Slack
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Fetching messages from Slack")
        logger.info("=" * 80)
        
        messages = slack_client.fetch_messages(Config.SLACK_CHANNEL)
        logger.info(f"Found {len(messages)} messages with PDF attachments")
        
        if not messages:
            logger.info("No PDF files found in channel. Pipeline complete.")
            return
        
        # Step 2: Download PDF files
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Downloading PDF files")
        logger.info("=" * 80)
        
        Config.PDF_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
        downloaded_files = slack_client.download_all_pdfs(messages, Config.PDF_STORAGE_PATH)
        logger.info(f"Downloaded {len(downloaded_files)} PDF files")
        
        if not downloaded_files:
            logger.info("No files to process. Pipeline complete.")
            return
        
        # Step 3: Process PDFs in parallel
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Processing PDFs in parallel")
        logger.info("=" * 80)
        
        processed_files = pdf_processor.process_pdfs(downloaded_files)
        logger.info(f"Processed {len(processed_files)} PDFs")
        
        # Step 4: Extract metadata using OpenAI
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Extracting metadata with OpenAI")
        logger.info("=" * 80)
        
        files_with_metadata = metadata_extractor.process_files(processed_files)
        logger.info(f"Extracted metadata for {len(files_with_metadata)} files")
        
        # Step 5: Store results in database
        logger.info("\n" + "=" * 80)
        logger.info("STEP 5: Storing results in database")
        logger.info("=" * 80)
        
        inserted_count = db_manager.bulk_insert_documents(files_with_metadata)
        logger.info(f"Stored {inserted_count} documents in database")
        
        # Display statistics
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE STATISTICS")
        logger.info("=" * 80)
        
        stats = db_manager.get_processing_stats()
        logger.info(f"Total documents in database: {stats.get('total_documents', 0)}")
        logger.info(f"Documents with title: {stats.get('documents_with_title', 0)}")
        logger.info(f"Documents with date: {stats.get('documents_with_date', 0)}")
        logger.info(f"Total text extracted: {stats.get('total_text_length', 0)} characters")
        logger.info(f"Total file size: {stats.get('total_file_size', 0)} bytes")
        
        elapsed_time = time.time() - start_time
        logger.info(f"\nPipeline completed successfully in {elapsed_time:.2f} seconds")
        
    except KeyboardInterrupt:
        logger.info("\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

