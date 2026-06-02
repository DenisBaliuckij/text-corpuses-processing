import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import patch, MagicMock
from repositories.proxy_repository import ProxyRepository

_CFG = {'ConnectionString': 'Driver={SQL Server};Server=test;'}


def test_proxy_add_or_update_calls_stored_proc():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        ProxyRepository.add_or_update('1.2.3.4', 8080, 12345, 'http')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[AddOrUpdateProxy] @ip = ?, @port = ?, @lastChecked = ?, @protocols = ?",
            ('1.2.3.4', 8080, 12345, 'http')
        )
        mock_conn.return_value.commit.assert_called_once()


def test_proxy_mark_broken_calls_stored_proc():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        ProxyRepository.mark_broken('1.2.3.4')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[MarkProxyAsBroken] @ip = ?", ('1.2.3.4',)
        )
        mock_conn.return_value.commit.assert_called_once()


def test_proxy_get_latest_returns_dict():
    with patch('repositories.proxy_repository.getConfig', return_value=_CFG), \
         patch('repositories.proxy_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        mock_cur.fetchone.return_value = ('1.2.3.4', 8080, 'http')
        result = ProxyRepository.get_latest()
        assert result == {'proxieIp': '1.2.3.4', 'proxiePort': 8080, 'proxieProtocol': 'http'}


from repositories.pdf_repository import PdfRepository
from repositories.latex_repository import LatexRepository


def test_pdf_add_url_calls_stored_proc():
    with patch('repositories.pdf_repository.getConfig', return_value=_CFG), \
         patch('repositories.pdf_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        PdfRepository.add_url('http://example.com/paper.pdf')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[AddPdfUrl] @url = ?", ('http://example.com/paper.pdf',)
        )


def test_pdf_get_next_to_download_returns_url():
    with patch('repositories.pdf_repository.getConfig', return_value=_CFG), \
         patch('repositories.pdf_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = ('http://arxiv.org/pdf/123',)
        result = PdfRepository.get_next_to_download()
        assert result == 'http://arxiv.org/pdf/123'


def test_pdf_get_next_to_download_returns_none_when_empty():
    with patch('repositories.pdf_repository.getConfig', return_value=_CFG), \
         patch('repositories.pdf_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = None
        result = PdfRepository.get_next_to_download()
        assert result is None


def test_pdf_save_location_calls_stored_proc():
    with patch('repositories.pdf_repository.getConfig', return_value=_CFG), \
         patch('repositories.pdf_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        PdfRepository.save_location('http://example.com/paper.pdf', 'arxiv/abc.pdf')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[SavePdfFileLocation] @pdfUrl = ?, @fileLocation=?",
            ('http://example.com/paper.pdf', 'arxiv/abc.pdf')
        )


def test_latex_get_next_to_convert_returns_path():
    with patch('repositories.latex_repository.getConfig', return_value=_CFG), \
         patch('repositories.latex_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = ('arxiv/paper.pdf',)
        result = LatexRepository.get_next_to_convert()
        assert result == 'arxiv/paper.pdf'


def test_latex_get_next_to_convert_returns_none_when_queue_empty():
    with patch('repositories.latex_repository.getConfig', return_value=_CFG), \
         patch('repositories.latex_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = (None,)
        result = LatexRepository.get_next_to_convert()
        assert result is None


def test_latex_save_location_calls_stored_proc():
    with patch('repositories.latex_repository.getConfig', return_value=_CFG), \
         patch('repositories.latex_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        LatexRepository.save_location('arxiv/paper.pdf', 'Tex/paper.tex')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[SaveLatexDocumentLocation] @pdfUrl = ?, @latexLocation=?",
            ('arxiv/paper.pdf', 'Tex/paper.tex')
        )


from repositories.graph_job_repository import GraphJobRepository


def test_graph_job_insert_job_calls_stored_proc():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        GraphJobRepository.insert_job('{"processorName":"RuleBased"}', 'arxiv/')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[AddGraphCreationJob] @config = ?, @paths=?",
            ('{"processorName":"RuleBased"}', 'arxiv/')
        )


def test_graph_job_get_for_preparation_returns_row():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = (1, 0, 'arxiv/')
        result = GraphJobRepository.get_job_for_preparation()
        assert result == (1, 0, 'arxiv/')


def test_graph_job_get_for_preparation_returns_none_when_empty():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = None
        result = GraphJobRepository.get_job_for_preparation()
        assert result is None


def test_graph_job_finalize_returns_job_row():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = (42,)
        result = GraphJobRepository.finalize_completed_jobs()
        assert result == (42,)
        mock_conn.return_value.cursor.return_value.execute.assert_called_once_with(
            "execute [dbo].[FinalizeCompletedJobs]"
        )


def test_graph_job_set_file_error_calls_stored_proc():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_cur = mock_conn.return_value.cursor.return_value
        GraphJobRepository.set_file_error(5, 'oops')
        mock_cur.execute.assert_called_once_with(
            "execute [dbo].[SetFileError] @fileId = ?, @error = ?", (5, 'oops')
        )


def test_graph_job_get_processor_config_returns_string():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchone.return_value = ('{"processorName":"RuleBased"}',)
        result = GraphJobRepository.get_processor_config(1)
        assert result == '{"processorName":"RuleBased"}'


def test_graph_job_get_files_for_job_returns_list():
    with patch('repositories.graph_job_repository.getConfig', return_value=_CFG), \
         patch('repositories.graph_job_repository.pyodbc.connect') as mock_conn:
        mock_conn.return_value.cursor.return_value.fetchall.return_value = [(1,), (2,)]
        result = GraphJobRepository.get_files_for_job(10)
        assert result == [(1,), (2,)]
