# -*- coding: utf-8 -*-
import functools
import json
import os
import sys

from flask import Flask, request, redirect, render_template, url_for, Response
from werkzeug.security import check_password_hash

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))

from configs import getConfig
from repositories.custom_query_repository import CustomQueryRepository

app = Flask(__name__)


class PrefixMiddleware:
    """Lets url_for() generate correct links when this app sits behind a
    reverse proxy that forwards /customquery/* here with the prefix
    stripped (e.g. `proxy_pass http://127.0.0.1:8090/;` under a
    `location /customquery/` block, with `proxy_set_header
    X-Forwarded-Prefix /customquery;`). Falls back to no prefix for direct
    access (e.g. curl to the container's own port), so both paths work."""
    def __init__(self, wsgi_app):
        self.wsgi_app = wsgi_app

    def __call__(self, environ, start_response):
        prefix = environ.get('HTTP_X_FORWARDED_PREFIX', '')
        if prefix:
            environ['SCRIPT_NAME'] = prefix
        return self.wsgi_app(environ, start_response)


app.wsgi_app = PrefixMiddleware(app.wsgi_app)

SOURCES = ['arxiv', 'pubmed', 'semantic_scholar', 'archive_org', 'shodhganga']

# Status codes, mirrored from Database/database-v0.17.sql
QUERY_STATUS_LABELS = {0: 'created', 20: 'fulfilling', 30: 'completed', 99: 'error'}
PDF_STATUS_LABELS = {0: 'pending', 10: 'copied (reused)', 20: 'downloaded', 99: 'failed'}


def require_auth(view):
    @functools.wraps(view)
    def wrapped(*args, **kwargs):
        auth = request.authorization
        config = getConfig()
        password_hash = config.get('CustomQueryUiPasswordHash', '')
        if not auth or not password_hash or not check_password_hash(password_hash, auth.password):
            return Response(
                'Authentication required', 401,
                {'WWW-Authenticate': 'Basic realm="Custom Query UI"'}
            )
        return view(*args, **kwargs)
    return wrapped


def _build_criterion(form, source_name):
    criterion = {}
    if source_name in ('arxiv', 'pubmed', 'semantic_scholar', 'archive_org'):
        criterion['query'] = form.get('query', '').strip()
    if source_name == 'shodhganga':
        criterion['subject'] = form.get('subject', '').strip()
        language = form.get('language', '').strip()
        if language:
            criterion['language'] = language
    if source_name == 'arxiv':
        categories = form.get('categories', '').strip()
        if categories:
            criterion['categories'] = [c.strip() for c in categories.split(',') if c.strip()]
    if source_name == 'semantic_scholar':
        fields_of_study = form.get('fields_of_study', '').strip()
        if fields_of_study:
            criterion['fields_of_study'] = [f.strip() for f in fields_of_study.split(',') if f.strip()]
        min_citations = form.get('min_citations', '').strip()
        if min_citations:
            criterion['min_citations'] = int(min_citations)
    if source_name in ('arxiv', 'pubmed', 'semantic_scholar'):
        date_from = form.get('date_from', '').strip()
        date_to = form.get('date_to', '').strip()
        if date_from:
            criterion['date_from'] = date_from
        if date_to:
            criterion['date_to'] = date_to
    if source_name in ('pubmed', 'semantic_scholar'):
        criterion['open_access_only'] = form.get('open_access_only') == 'on'
    max_results = form.get('max_results', '').strip()
    criterion['max_results'] = int(max_results) if max_results else 200
    return criterion


@app.route('/')
@require_auth
def index():
    recent = CustomQueryRepository.get_recent()
    return render_template(
        'index.html', sources=SOURCES, recent=recent,
        query_status_labels=QUERY_STATUS_LABELS,
    )


@app.route('/submit', methods=['POST'])
@require_auth
def submit():
    source_name = request.form.get('source_name', '')
    folder_name = request.form.get('folder_name', '').strip()
    if source_name not in SOURCES or not folder_name:
        return 'source_name and folder_name are required', 400

    criterion = _build_criterion(request.form, source_name)
    query_id = CustomQueryRepository.create(source_name, json.dumps(criterion), folder_name)
    return redirect(url_for('query_status', query_id=query_id))


@app.route('/query/<int:query_id>')
@require_auth
def query_status(query_id):
    row = CustomQueryRepository.get_status(query_id)
    if row is None:
        return 'Not found', 404
    pdfs = CustomQueryRepository.get_pdfs(query_id)
    return render_template(
        'query_status.html', row=row, pdfs=pdfs,
        query_status_labels=QUERY_STATUS_LABELS, pdf_status_labels=PDF_STATUS_LABELS,
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
