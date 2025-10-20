"""PostgreSQL database manager for storing document metadata."""
import logging
from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

from config import Config

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manage PostgreSQL database operations for document metadata."""
    
    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        """Initialize database manager.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
    
    def get_connection(self):
        """Get a database connection."""
        return psycopg2.connect(**self.connection_params)
    
    def init_schema(self) -> None:
        """Initialize database schema.
        
        Schema design:
        - documents table: stores document metadata
        - file_id is unique to ensure idempotency
        - Includes all relevant metadata from Slack and OpenAI
        - timestamps for tracking
        """
        schema_sql = """
        CREATE TABLE IF NOT EXISTS documents (
            id SERIAL PRIMARY KEY,
            file_id VARCHAR(255) UNIQUE NOT NULL,
            file_name VARCHAR(500) NOT NULL,
            title TEXT,
            publication_date DATE,
            extracted_text_path TEXT,
            text_length INTEGER,
            slack_url TEXT,
            message_ts VARCHAR(50),
            message_text TEXT,
            file_size INTEGER,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Index for faster lookups
        CREATE INDEX IF NOT EXISTS idx_file_id ON documents(file_id);
        CREATE INDEX IF NOT EXISTS idx_processed_at ON documents(processed_at);
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(schema_sql)
                    conn.commit()
            logger.info("Database schema initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing schema: {e}")
            raise
    
    def document_exists(self, file_id: str) -> bool:
        """Check if a document already exists in the database.
        
        Args:
            file_id: Slack file ID
            
        Returns:
            True if document exists, False otherwise
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM documents WHERE file_id = %s", (file_id,))
                    return cur.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking document existence: {e}")
            return False
    
    def insert_document(self, document: Dict[str, Any]) -> Optional[int]:
        """Insert a document into the database.
        
        Implements idempotency using ON CONFLICT DO UPDATE.
        If a document with the same file_id exists, it will be updated.
        
        Args:
            document: Dictionary containing document metadata
            
        Returns:
            Document ID if successful, None otherwise
        """
        insert_sql = """
        INSERT INTO documents (
            file_id, file_name, title, publication_date,
            extracted_text_path, text_length, slack_url,
            message_ts, message_text, file_size
        ) VALUES (
            %(file_id)s, %(file_name)s, %(title)s, %(publication_date)s,
            %(extracted_text_path)s, %(text_length)s, %(slack_url)s,
            %(message_ts)s, %(message_text)s, %(file_size)s
        )
        ON CONFLICT (file_id) 
        DO UPDATE SET
            title = EXCLUDED.title,
            publication_date = EXCLUDED.publication_date,
            extracted_text_path = EXCLUDED.extracted_text_path,
            text_length = EXCLUDED.text_length,
            updated_at = CURRENT_TIMESTAMP
        RETURNING id;
        """
        
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    # Prepare document data
                    doc_data = {
                        'file_id': document['file_id'],
                        'file_name': document['file_name'],
                        'title': document.get('title'),
                        'publication_date': document.get('publication_date'),
                        'extracted_text_path': str(document.get('text_file_path', '')),
                        'text_length': document.get('text_length', 0),
                        'slack_url': document.get('slack_url', ''),
                        'message_ts': document.get('message_ts', ''),
                        'message_text': document.get('message_text', ''),
                        'file_size': document.get('file_size', 0)
                    }
                    
                    cur.execute(insert_sql, doc_data)
                    result = cur.fetchone()
                    conn.commit()
                    
                    doc_id = result[0] if result else None
                    logger.info(f"Inserted/updated document {document['file_id']} with ID {doc_id}")
                    return doc_id
                    
        except Exception as e:
            logger.error(f"Error inserting document: {e}")
            return None
    
    def bulk_insert_documents(self, documents: List[Dict[str, Any]]) -> int:
        """Insert multiple documents into the database.
        
        Args:
            documents: List of document dictionaries
            
        Returns:
            Number of documents successfully inserted/updated
        """
        success_count = 0
        
        for document in documents:
            doc_id = self.insert_document(document)
            if doc_id:
                success_count += 1
        
        logger.info(f"Successfully inserted/updated {success_count}/{len(documents)} documents")
        return success_count
    
    def get_all_documents(self) -> List[Dict[str, Any]]:
        """Retrieve all documents from the database.
        
        Returns:
            List of document dictionaries
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("SELECT * FROM documents ORDER BY processed_at DESC")
                    return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error retrieving documents: {e}")
            return []
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get statistics about processed documents.
        
        Returns:
            Dictionary with processing statistics
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total_documents,
                            COUNT(title) as documents_with_title,
                            COUNT(publication_date) as documents_with_date,
                            SUM(text_length) as total_text_length,
                            SUM(file_size) as total_file_size
                        FROM documents
                    """)
                    return dict(cur.fetchone())
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {}

