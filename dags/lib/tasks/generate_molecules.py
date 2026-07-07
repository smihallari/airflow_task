import argparse
import io
import itertools
import logging
import re
import sys

import pandas as pd
from rdkit import Chem

from lib.utils.aws import get_s3_client

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def normalize_attachment(smi: str) -> str:
    """Forces all generic * or labeled [*:N] attachment points to [*:1]"""
    return re.sub(r"\[\*\:\d+\]|\*", "[*:1]", smi)


def generate_combinatorial_library(scaffolds: list[str], r_groups: list[str]) -> list[dict]:
    results = []

    for scaffold, r_group in itertools.product(scaffolds, r_groups):
        try:
            s_smi = normalize_attachment(scaffold)
            r_smi = normalize_attachment(r_group)

            # Combine into a single disconnected string, then zip
            combined_smiles = f"{s_smi}.{r_smi}"
            mol = Chem.MolFromSmiles(combined_smiles)

            if mol is None:
                continue

            prod = Chem.molzip(mol)
            Chem.SanitizeMol(prod)

            # Verify no disconnected fragments remain (no '.' in final SMILES)
            final_smiles = Chem.MolToSmiles(prod)
            if "." not in final_smiles:
                results.append({"scaffold": scaffold, "r_group": r_group, "smiles": final_smiles})
        except Exception as e:
            logger.debug(f"Failed to merge {scaffold} and {r_group}: {e}")
            continue

    return results


def main():
    parser = argparse.ArgumentParser(description="Stateless molecule generation.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--scaffold-key", required=True)
    parser.add_argument("--rgroup-key", required=True)
    parser.add_argument("--output-key", required=True)
    args = parser.parse_args()

    s3 = get_s3_client()

    try:
        scaffold_raw = s3.get_object(Bucket=args.bucket, Key=args.scaffold_key)["Body"].read()
        rgroup_raw = s3.get_object(Bucket=args.bucket, Key=args.rgroup_key)["Body"].read()
    except Exception as e:
        logger.error(f"Failed to download input files from S3: {e}")
        sys.exit(1)

    scaffolds_df = pd.read_csv(io.BytesIO(scaffold_raw))
    rgroups_df = pd.read_csv(io.BytesIO(rgroup_raw))

    # Extract valid strings, ignoring empties
    scaffold_list = [s.strip() for s in scaffolds_df.get("smiles", []) if pd.notna(s)]
    rgroup_list = [s.strip() for s in rgroups_df.get("smiles", []) if pd.notna(s)]

    results = generate_combinatorial_library(scaffold_list, rgroup_list)

    if not results:
        logger.error("Generation produced 0 molecules. Stopping execution.")
        sys.exit(1)

    out_df = pd.DataFrame(results)
    csv_buffer = io.StringIO()
    out_df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=args.bucket,
        Key=args.output_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info(f"Uploaded {len(results)} generated molecules to {args.output_key}")


if __name__ == "__main__":
    main()
