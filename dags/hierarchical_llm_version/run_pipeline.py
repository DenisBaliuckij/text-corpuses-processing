#!/usr/bin/env python3
"""CLI entry point: python run_pipeline.py --config config.yaml"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_THIS_DIR = Path(__file__).parent
_PARENT_DIR = _THIS_DIR.parent
if str(_PARENT_DIR) not in sys.path:
    sys.path.insert(0, str(_PARENT_DIR))

from hierarchical_llm_version.config_schema import load_config
from hierarchical_llm_version.pipeline import Pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Hierarchical LLM Semantic Graph Builder")
    parser.add_argument("--config", type=str, default="config.yaml")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config_path = Path(args.config)
    base_dir = config_path.parent.resolve()
    config = load_config(config_path)

    pipe = Pipeline(config, base_dir=base_dir)
    result = pipe.run()

    raw = result["raw_graph"]
    clust = result["clustered_graph"]
    stats = result["token_stats"]
    print()
    print(f"Done.")
    print(f"  raw graph:       {len(raw.nodes)} nodes, {len(raw.edges)} edges")
    print(f"  clustered graph: {len(clust.nodes)} nodes, {len(clust.edges)} edges")
    print(f"  tokens:          input={stats['total_input_tokens']}  output={stats['total_output_tokens']}  calls={stats['total_calls']}")
    print(f"  hierarchy:       levels={result['hierarchy_stats']['level_sizes']}")
    print(f"  outputs:         {config.paths.output_dir}/")


if __name__ == "__main__":
    main()
