import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import io
from unittest.mock import patch

import pytest

from sciparse_bridge import ConversionTimeoutError, register_and_wait


def test_register_and_wait_uploads_registers_and_returns_stripped_text(tmp_path):
    pdf = tmp_path / "book.pdf"
    pdf.write_bytes(b"%PDF fake")

    with patch('sciparse_bridge.ftpConnector') as mock_ftp, \
         patch('sciparse_bridge.LatexRepository') as mock_repo, \
         patch('sciparse_bridge.time.sleep'):
        mock_repo.get_latex_location.return_value = 'Tex/book.tex'
        mock_ftp.getFile.return_value = io.BytesIO(r"\section{Hello}".encode('utf-8'))

        result = register_and_wait(pdf, 'mr', poll_interval_s=1, timeout_s=10)

        mock_ftp.storeFile.assert_called_once()
        assert mock_ftp.storeFile.call_args[0][0] == 'langembed_bridge/mr/book.pdf'
        mock_repo.register_for_conversion.assert_called_once_with(
            pdf_url='langembed-bridge:mr/book.pdf',
            location_in_filesystem='langembed_bridge/mr/book.pdf',
        )
        mock_ftp.getFile.assert_called_once_with('Tex/book.tex', 'Tex')
        assert result == 'Hello'


def test_register_and_wait_polls_until_ready(tmp_path):
    pdf = tmp_path / "book.pdf"
    pdf.write_bytes(b"%PDF fake")

    with patch('sciparse_bridge.ftpConnector') as mock_ftp, \
         patch('sciparse_bridge.LatexRepository') as mock_repo, \
         patch('sciparse_bridge.time.sleep') as mock_sleep:
        mock_repo.get_latex_location.side_effect = [None, None, 'Tex/book.tex']
        mock_ftp.getFile.return_value = io.BytesIO(b"text")

        register_and_wait(pdf, 'mr', poll_interval_s=1, timeout_s=100)

        assert mock_repo.get_latex_location.call_count == 3
        assert mock_sleep.call_count == 2


def test_register_and_wait_times_out(tmp_path):
    pdf = tmp_path / "book.pdf"
    pdf.write_bytes(b"%PDF fake")

    with patch('sciparse_bridge.ftpConnector') as mock_ftp, \
         patch('sciparse_bridge.LatexRepository') as mock_repo, \
         patch('sciparse_bridge.time.monotonic', side_effect=[0, 1, 2, 100]), \
         patch('sciparse_bridge.time.sleep'):
        mock_repo.get_latex_location.return_value = None

        with pytest.raises(ConversionTimeoutError):
            register_and_wait(pdf, 'mr', poll_interval_s=1, timeout_s=10)
