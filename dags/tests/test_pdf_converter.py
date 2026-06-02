import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import patch, MagicMock
import pdfConverter


def test_empty_queue_returns_zero():
    with patch('pdfConverter.databaseConnector.getPdfToConvertToLatex', return_value=None):
        result = pdfConverter.run_conversion()
    assert result == 0


def test_converts_pdf_and_returns_count():
    mock_file = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "hello world"
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]
    call_order = []
    mock_file.seek = MagicMock(side_effect=lambda n: call_order.append('seek'))

    def mock_pdf_reader(f):
        call_order.append('PdfReader')
        return mock_reader

    with patch('pdfConverter.databaseConnector.getPdfToConvertToLatex', side_effect=['arxiv/paper.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', return_value=mock_file), \
         patch('pdfConverter.PdfReader', side_effect=mock_pdf_reader), \
         patch('pdfConverter.ftpConnector.storeFile') as mock_store, \
         patch('pdfConverter.databaseConnector.saveLatexFileLocation') as mock_save:
        result = pdfConverter.run_conversion()

    assert result == 1
    assert call_order == ['seek', 'PdfReader'], f"Expected seek before PdfReader, got: {call_order}"
    assert mock_store.call_args[0][0] == 'arxiv/paper.tex'
    mock_save.assert_called_once_with('arxiv/paper.pdf', 'arxiv/paper.tex')


def test_failed_conversion_saves_na_and_loop_continues():
    mock_bad_file = MagicMock()
    mock_bad_file.seek.side_effect = Exception("FTP read error")

    mock_good_file = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "good text"
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]

    with patch('pdfConverter.databaseConnector.getPdfToConvertToLatex', side_effect=['bad/file.pdf', 'good/file.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', side_effect=[mock_bad_file, mock_good_file]), \
         patch('pdfConverter.PdfReader', return_value=mock_reader), \
         patch('pdfConverter.ftpConnector.storeFile'), \
         patch('pdfConverter.databaseConnector.saveLatexFileLocation') as mock_save:
        result = pdfConverter.run_conversion()

    assert result == 1
    assert mock_save.call_args_list[0][0] == ('bad/file.pdf', 'NA')
    assert mock_save.call_args_list[1][0] == ('good/file.pdf', 'good/file.tex')


def test_none_page_text_handled():
    mock_file = MagicMock()
    mock_page = MagicMock()
    mock_page.extract_text.return_value = None
    mock_reader = MagicMock()
    mock_reader.pages = [mock_page]

    with patch('pdfConverter.databaseConnector.getPdfToConvertToLatex', side_effect=['arxiv/paper.pdf', None]), \
         patch('pdfConverter.ftpConnector.getFile', return_value=mock_file), \
         patch('pdfConverter.PdfReader', return_value=mock_reader), \
         patch('pdfConverter.ftpConnector.storeFile'), \
         patch('pdfConverter.databaseConnector.saveLatexFileLocation'):
        result = pdfConverter.run_conversion()

    assert result == 1
