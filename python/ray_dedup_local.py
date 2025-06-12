import argparse
from orchestration import main as orchestrate


def cli():
    parser = argparse.ArgumentParser(
        description="Run minhash dedup on a Ray cluster using local files"
    )
    parser.add_argument(
        "--config", required=True, help="Path to YAML config with input/output paths"
    )
    args = parser.parse_args()
    orchestrate(args.config)


if __name__ == "__main__":
    cli()
