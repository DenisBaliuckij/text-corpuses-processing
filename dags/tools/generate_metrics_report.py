# dags/tools/generate_metrics_report.py
"""
Usage: python dags/tools/generate_metrics_report.py <job_id>

Fetches all metrics.json files for a completed job from FTP,
produces a self-contained metrics_report.html, and saves it to FTP.
Prints the FTP path on success.
"""
import sys
import os
import json
import io

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dbConnector import databaseConnector
from ftpConnector import ftpConnector

# Chart.js CDN (pinned version, small, works offline if cached; swap for inline bundle if needed)
_CHARTJS = "https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"


def _fetch_metrics(job_id: int) -> list[dict]:
    config_json = databaseConnector.getProcessorConfig(job_id)
    config = json.loads(config_json) if config_json else {}
    processor = config.get("processorName", "RuleBased")

    entries = []

    if processor == "RuleBased":
        raw = ftpConnector.getFile(f"graphJobs/{job_id}/metrics.json", 'Graph')
        raw.seek(0)
        m = json.loads(raw.read().decode('utf-8'))
        entries.append({"file_id": "job", "metrics": m})
    else:
        prefix = "llm_v2" if processor != "Hierarchical" else "hierarchical"
        file_rows = databaseConnector.getFilesForJob(job_id)
        for row in file_rows:
            file_id = row[0]
            path = f"graphJobs/{job_id}/{prefix}/{file_id}/metrics.json"
            try:
                raw = ftpConnector.getFile(path, 'Graph')
                raw.seek(0)
                m = json.loads(raw.read().decode('utf-8'))
                entries.append({"file_id": file_id, "metrics": m})
            except Exception as e:
                print(f"Warning: could not fetch metrics for file {file_id}: {e}", file=sys.stderr)

    return entries


def _render_html(job_id: int, entries: list[dict]) -> str:
    rows = ""
    chart_datasets = []
    chart_labels = []

    for entry in entries:
        fid = entry["file_id"]
        m = entry["metrics"]
        rows += f"""
        <tr>
          <td>{fid}</td>
          <td>{m.get('node_count', '-')}</td>
          <td>{m.get('edge_count', '-')}</td>
          <td>{m.get('density', 0):.4f}</td>
          <td>{m.get('avg_degree', 0):.2f}</td>
          <td>{m.get('avg_clustering_coefficient', 0):.4f}</td>
          <td>{m.get('connected_components', '-')}</td>
          <td>{m.get('diameter') or 'N/A'}</td>
        </tr>"""

        deg_dist = m.get("degree_distribution", {})
        if chart_labels == []:
            chart_labels = sorted(deg_dist.keys(), key=lambda x: int(x))
        chart_datasets.append({
            "label": f"File {fid}",
            "data": [deg_dist.get(k, 0) for k in chart_labels],
            "borderWidth": 1,
        })

    hub_sections = ""
    for entry in entries:
        fid = entry["file_id"]
        hubs = entry["metrics"].get("top_10_hubs", [])
        hub_rows = "".join(
            f"<tr><td>{h['label']}</td><td>{h['degree']}</td></tr>"
            for h in hubs
        )
        hub_sections += f"""
        <h3>Top hubs — file {fid}</h3>
        <table><thead><tr><th>Node</th><th>Degree</th></tr></thead>
        <tbody>{hub_rows}</tbody></table>"""

    total_nodes = sum(e["metrics"].get("node_count", 0) for e in entries)
    total_edges = sum(e["metrics"].get("edge_count", 0) for e in entries)
    densities = [e["metrics"].get("density", 0) for e in entries]
    mean_density = sum(densities) / len(densities) if densities else 0

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Metrics Report — Job {job_id}</title>
<script src="{_CHARTJS}"></script>
<style>
  body {{ font-family: monospace; background: #1a1a2e; color: #e0e0ff; padding: 2rem; }}
  h1, h2, h3 {{ color: #6c63ff; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
  th, td {{ border: 1px solid #333; padding: 6px 12px; text-align: left; }}
  th {{ background: #2d2d4e; }}
  .summary {{ display: flex; gap: 2rem; margin-bottom: 2rem; }}
  .stat {{ background: #2d2d4e; padding: 1rem 2rem; border-radius: 8px; }}
  .stat-value {{ font-size: 2rem; color: #6c63ff; }}
  canvas {{ max-height: 300px; margin-bottom: 2rem; }}
</style>
</head>
<body>
<h1>Graph Metrics Report</h1>
<p>Job ID: <strong>{job_id}</strong> &nbsp;·&nbsp; {len(entries)} file(s)</p>
<div class="summary">
  <div class="stat"><div class="stat-value">{total_nodes}</div>Total nodes</div>
  <div class="stat"><div class="stat-value">{total_edges}</div>Total edges</div>
  <div class="stat"><div class="stat-value">{mean_density:.4f}</div>Mean density</div>
</div>
<h2>Per-file metrics</h2>
<table>
  <thead><tr>
    <th>File</th><th>Nodes</th><th>Edges</th><th>Density</th>
    <th>Avg degree</th><th>Clustering</th><th>Components</th><th>Diameter</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>
<h2>Degree distribution</h2>
<canvas id="degChart"></canvas>
<script>
new Chart(document.getElementById('degChart'), {{
  type: 'bar',
  data: {{
    labels: {json.dumps(chart_labels)},
    datasets: {json.dumps(chart_datasets)}
  }},
  options: {{ responsive: true, plugins: {{ legend: {{ labels: {{ color: '#e0e0ff' }} }} }},
    scales: {{ x: {{ ticks: {{ color: '#e0e0ff' }} }}, y: {{ ticks: {{ color: '#e0e0ff' }} }} }} }}
}});
</script>
{hub_sections}
</body></html>"""


def main():
    if len(sys.argv) < 2:
        print("Usage: python generate_metrics_report.py <job_id>", file=sys.stderr)
        sys.exit(1)

    job_id = int(sys.argv[1])
    entries = _fetch_metrics(job_id)
    if not entries:
        print(f"No metrics found for job {job_id}.", file=sys.stderr)
        sys.exit(1)

    html = _render_html(job_id, entries)
    report_path = f"graphJobs/{job_id}/metrics_report.html"
    ftpConnector.storeFile(report_path, io.BytesIO(html.encode('utf-8')), 'Graph')
    print(f"Report saved to FTP: {report_path}")


if __name__ == "__main__":
    main()
