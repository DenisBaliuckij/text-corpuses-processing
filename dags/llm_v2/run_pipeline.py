#!/usr/bin/env python3
"""CLI entry point: python run_pipeline.py --config config.yaml"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from config_schema import load_config
from pipeline import Pipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Semantic Graph Builder v2")
    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    config_path = Path(args.config)
    base_dir = config_path.parent.resolve()

    config = load_config(config_path)
    pipe = Pipeline(config, base_dir=base_dir)
    result = pipe.run()

    raw_g = result["raw_graph"]
    clust_g = result["clustered_graph"]
    print(f"\nDone. Raw graph: {len(raw_g.nodes)} nodes, {len(raw_g.edges)} edges")
    print(f"Clustered graph: {len(clust_g.nodes)} nodes, {len(clust_g.edges)} edges")
    print(f"Outputs saved to: {config.paths.output_dir}/")


if __name__ == "__main__":
    main()
