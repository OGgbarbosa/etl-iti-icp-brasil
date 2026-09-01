from __future__ import annotations

import argparse


def main() -> None:
    """Entry point principal do pacote ITI_ICP_BRASIL."""
    parser = argparse.ArgumentParser(
        description="ETL ITI - ICP-Brasil Pipeline Runner",
    )
    parser.add_argument("--catalog", required=False, default="workspace")
    parser.add_argument("--schema", required=False, default="default")
    args = parser.parse_args()

    print(f"Iniciando execucao do pacote ITI_ICP_BRASIL no catalogo '{args.catalog}' e schema '{args.schema}'...")


if __name__ == "__main__":
    main()
