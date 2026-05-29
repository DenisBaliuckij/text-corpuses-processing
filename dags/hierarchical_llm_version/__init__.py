"""hierarchical_llm_version

Two-pass semantic graph builder.

Pass 1 builds a hierarchical overview tree of the corpus (chunk summaries ->
recursive aggregation -> root). Pass 2 runs triplet extraction on each chunk
with a per-chunk global context derived from the tree, then performs global
entity resolution.

Output schemas (raw_graph.json / clustered_graph.json) match `llm_v2`.
"""
