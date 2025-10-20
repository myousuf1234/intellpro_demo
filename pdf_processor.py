"""PDF text extraction using PyMuPDF with multiprocessing."""
import logging
from pathlib import Path
from typing import Dict, List, Any
from multiprocessing import Pool, cpu_count
import fitz  # PyMuPDF

from config import Config

logger = logging.getLogger(__name__)


def extract_text_from_pdf(pdf_path: Path) -> str:
    """Extract text from a single PDF file using PyMuPDF.
    
    This function is designed to be used with multiprocessing.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        Extracted text content
    """
    try:
        logger.info(f"Extracting text from {pdf_path.name}...")
        
        doc = fitz.open(pdf_path)
        text_content = []
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            text_content.append(page.get_text())
        
        doc.close()
        
        full_text = "\n".join(text_content)
        logger.info(f"Extracted {len(full_text)} characters from {pdf_path.name}")
        
        return full_text
        
    except Exception as e:
        logger.error(f"Error extracting text from {pdf_path}: {e}")
        return ""


def process_single_pdf(file_info: Dict[str, Any]) -> Dict[str, Any]:
    """Process a single PDF file (wrapper for multiprocessing).
    
    Args:
        file_info: Dictionary containing file metadata and local_path
        
    Returns:
        Dictionary with file info and extracted text
    """
    pdf_path = file_info['local_path']
    extracted_text = extract_text_from_pdf(pdf_path)
    
    # Save extracted text to a separate file for auditing
    text_file_path = pdf_path.with_suffix('.txt')
    try:
        with open(text_file_path, 'w', encoding='utf-8') as f:
            f.write(extracted_text)
        logger.info(f"Saved extracted text to {text_file_path}")
    except Exception as e:
        logger.error(f"Error saving text file: {e}")
    
    return {
        **file_info,
        'extracted_text': extracted_text,
        'text_file_path': text_file_path,
        'text_length': len(extracted_text)
    }


class PDFProcessor:
    """Process multiple PDFs in parallel using multiprocessing."""
    
    def __init__(self, max_workers: int = None):
        """Initialize PDF processor.
        
        Args:
            max_workers: Maximum number of worker processes. 
                        Defaults to CPU count or Config.MAX_WORKERS
        """
        if max_workers is None:
            max_workers = min(Config.MAX_WORKERS, cpu_count())
        
        self.max_workers = max_workers
        logger.info(f"Initialized PDFProcessor with {self.max_workers} workers")
    
    def process_pdfs(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Process multiple PDF files in parallel.
        
        This demonstrates proper use of multiprocessing for CPU-bound tasks.
        Each PDF is processed in a separate process to bypass Python's GIL.
        
        Args:
            files: List of file info dictionaries with local_path
            
        Returns:
            List of processed file dictionaries with extracted text
        """
        if not files:
            logger.warning("No files to process")
            return []
        
        logger.info(f"Processing {len(files)} PDFs with {self.max_workers} workers...")
        
        # Use multiprocessing Pool for parallel processing
        # This is effective for CPU-bound PDF text extraction
        with Pool(processes=self.max_workers) as pool:
            processed_files = pool.map(process_single_pdf, files)
        
        logger.info(f"Completed processing {len(processed_files)} PDFs")
        
        # Log statistics
        total_chars = sum(f['text_length'] for f in processed_files)
        logger.info(f"Total characters extracted: {total_chars}")
        
        return processed_files

