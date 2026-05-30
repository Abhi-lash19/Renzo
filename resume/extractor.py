"""
resume/extractor.py — Extract raw text from PDF, DOCX, or plain-text resume files.

All functions accept raw bytes (from file upload or BytesIO) rather than file paths,
keeping this module stateless and easily testable.
"""
from __future__ import annotations

import io
from utils.logger import get_logger

logger = get_logger(__name__)

SUPPORTED_EXTENSIONS = {".pdf", ".docx", ".doc", ".txt"}


def _pdfminer_extract(file_bytes: bytes) -> str:
    """Internal: call pdfminer high-level extractor. Separated for testability."""
    from pdfminer.high_level import extract_text as pdfminer_extract_text
    return pdfminer_extract_text(io.BytesIO(file_bytes)) or ""


def _docx_extract(file_bytes: bytes) -> str:
    """Internal: call python-docx extractor. Separated for testability."""
    from docx import Document
    doc = Document(io.BytesIO(file_bytes))
    paragraphs = [para.text for para in doc.paragraphs if para.text.strip()]
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                if cell.text.strip():
                    paragraphs.append(cell.text.strip())
    return "\n".join(paragraphs)


def extract_text_from_pdf(file_bytes: bytes) -> str:
    """
    Extract plain text from a PDF file.

    Args:
        file_bytes: Raw PDF file content.

    Returns:
        Extracted text string, stripped of leading/trailing whitespace.

    Raises:
        ValueError: If the PDF cannot be read or parsed.
    """
    if not file_bytes:
        return ""
    try:
        text = _pdfminer_extract(file_bytes)
        logger.debug(f"[EXTRACTOR] PDF extracted {len(text)} chars")
        return text.strip()
    except Exception as e:
        logger.error(f"[EXTRACTOR] PDF extraction failed: {e}")
        raise ValueError(f"PDF extraction failed: {e}") from e


def extract_text_from_docx(file_bytes: bytes) -> str:
    """
    Extract plain text from a DOCX file.

    Args:
        file_bytes: Raw DOCX file content.

    Returns:
        Extracted text string.

    Raises:
        ValueError: If the DOCX cannot be read or parsed.
    """
    if not file_bytes:
        return ""
    try:
        text = _docx_extract(file_bytes)
        logger.debug(f"[EXTRACTOR] DOCX extracted {len(text)} chars")
        return text.strip()
    except Exception as e:
        logger.error(f"[EXTRACTOR] DOCX extraction failed: {e}")
        raise ValueError(f"DOCX extraction failed: {e}") from e


def extract_text(file_bytes: bytes, filename: str) -> str:
    """
    Dispatch to the correct extractor based on file extension.

    Supported: .pdf, .docx, .doc (treated as DOCX), .txt

    Args:
        file_bytes: Raw file content.
        filename:   Original filename (used to determine format).

    Returns:
        Extracted plain text.

    Raises:
        ValueError: For unsupported file types or extraction failures.
    """
    if not file_bytes:
        return ""

    ext = ("." + filename.rsplit(".", 1)[-1].lower()) if "." in filename else ""

    if ext == ".pdf":
        return extract_text_from_pdf(file_bytes)
    elif ext in (".docx", ".doc"):
        return extract_text_from_docx(file_bytes)
    elif ext == ".txt":
        try:
            return file_bytes.decode("utf-8", errors="replace").strip()
        except Exception as e:
            raise ValueError(f"Text file decoding failed: {e}") from e
    else:
        raise ValueError(
            f"Unsupported file type: '{ext}'. "
            f"Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        )
