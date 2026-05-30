import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import patch


class TestExtractText:
    def test_txt_file_returns_decoded_text(self):
        from resume.extractor import extract_text
        content = b"Hello, World! Python developer."
        result = extract_text(content, "resume.txt")
        assert "Hello" in result
        assert "Python" in result

    def test_unsupported_extension_raises_value_error(self):
        from resume.extractor import extract_text
        with pytest.raises(ValueError, match="Unsupported"):
            extract_text(b"data", "resume.xlsx")

    def test_uppercase_extension_handled(self):
        from resume.extractor import extract_text
        content = b"Software engineer with Python experience."
        result = extract_text(content, "RESUME.TXT")
        assert "Python" in result

    def test_pdf_dispatches_to_pdf_extractor(self):
        from resume.extractor import extract_text
        with patch("resume.extractor.extract_text_from_pdf", return_value="pdf text") as mock:
            result = extract_text(b"fake pdf bytes", "resume.pdf")
        mock.assert_called_once()
        assert result == "pdf text"

    def test_docx_dispatches_to_docx_extractor(self):
        from resume.extractor import extract_text
        with patch("resume.extractor.extract_text_from_docx", return_value="docx text") as mock:
            result = extract_text(b"fake docx bytes", "resume.docx")
        mock.assert_called_once()
        assert result == "docx text"

    def test_empty_bytes_returns_empty_string(self):
        from resume.extractor import extract_text
        result = extract_text(b"", "resume.txt")
        assert result == ""

    def test_doc_extension_uses_docx_extractor(self):
        from resume.extractor import extract_text
        with patch("resume.extractor.extract_text_from_docx", return_value="doc text") as mock:
            result = extract_text(b"fake doc bytes", "resume.doc")
        mock.assert_called_once()
        assert result == "doc text"


class TestExtractTextFromPdf:
    def test_returns_string(self):
        from resume.extractor import extract_text_from_pdf
        with patch("resume.extractor._pdfminer_extract", return_value="mocked pdf content"):
            result = extract_text_from_pdf(b"fake pdf bytes")
        assert isinstance(result, str)
        assert "mocked pdf content" in result

    def test_extraction_error_raises_value_error(self):
        from resume.extractor import extract_text_from_pdf
        with patch("resume.extractor._pdfminer_extract", side_effect=Exception("corrupt pdf")):
            with pytest.raises(ValueError, match="PDF extraction failed"):
                extract_text_from_pdf(b"bad bytes")

    def test_empty_bytes_returns_empty_string(self):
        from resume.extractor import extract_text_from_pdf
        result = extract_text_from_pdf(b"")
        assert result == ""


class TestExtractTextFromDocx:
    def test_returns_string(self):
        from resume.extractor import extract_text_from_docx
        with patch("resume.extractor._docx_extract", return_value="mocked docx content"):
            result = extract_text_from_docx(b"fake docx bytes")
        assert isinstance(result, str)

    def test_extraction_error_raises_value_error(self):
        from resume.extractor import extract_text_from_docx
        with patch("resume.extractor._docx_extract", side_effect=Exception("corrupt docx")):
            with pytest.raises(ValueError, match="DOCX extraction failed"):
                extract_text_from_docx(b"bad bytes")

    def test_empty_bytes_returns_empty_string(self):
        from resume.extractor import extract_text_from_docx
        result = extract_text_from_docx(b"")
        assert result == ""
