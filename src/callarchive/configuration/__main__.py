import argparse
from pathlib import Path

from . import config_from_args


def main():
    parser = argparse.ArgumentParser(description="Validate a CallArchive configuration file.")
    parser.add_argument("--config", type=Path, required=True)
    config_from_args(parser, parser.parse_args())
    print("Configuration is valid.")


if __name__ == "__main__":
    main()
