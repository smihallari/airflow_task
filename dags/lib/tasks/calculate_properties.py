import argparse
import csv
import io
import logging
import sys
from typing import Any

from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors

from lib.utils.aws import get_s3_client

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

PROPERTIES: dict[str, Any] = {
    "mol_weight": Descriptors.MolWt,
    "log_p": Descriptors.MolLogP,
    "tpsa": Descriptors.TPSA,
    "hba": rdMolDescriptors.CalcNumHBA,
    "hbd": rdMolDescriptors.CalcNumHBD,
    "rotatable_bonds": rdMolDescriptors.CalcNumRotatableBonds,
    "aromatic_rings": rdMolDescriptors.CalcNumAromaticRings,
}


def main():
    parser = argparse.ArgumentParser(description="Stateless molecular properties calculation.")
    parser.add_argument("--bucket", required=True, help="S3/MinIO bucket name")
    parser.add_argument(
        "--input-key", required=True, help="S3 key for the input generated molecules CSV"
    )
    parser.add_argument(
        "--output-key", required=True, help="S3 destination key for calculated properties CSV"
    )
    parser.add_argument("--smiles-col", default="smiles", help="Name of the SMILES column")
    args = parser.parse_args()

    s3 = get_s3_client()

    # 1. Download input file
    try:
        response = s3.get_object(Bucket=args.bucket, Key=args.input_key)
        raw_data = response["Body"].read().decode("utf-8")
    except Exception as e:
        logger.error(f"Failed to fetch file from S3: {e}")
        sys.exit(1)

    # 2. Process data and compute chemistry metrics
    reader = csv.DictReader(io.StringIO(raw_data))
    if args.smiles_col not in (reader.fieldnames or []):
        logger.error(f"SMILES column '{args.smiles_col}' not found in input CSV.")
        sys.exit(1)

    result_rows = []
    for row in reader:
        smi = row.get(args.smiles_col, "").strip()
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            logger.warning(f"Skipping invalid SMILES: {smi!r}")
            continue

        props = {name: round(fn(mol), 4) for name, fn in PROPERTIES.items()}
        props["lipinski_pass"] = int(
            props["mol_weight"] <= 500
            and props["log_p"] <= 5
            and props["hba"] <= 10
            and props["hbd"] <= 5
        )
        result_rows.append({**row, **props})

    if not result_rows:
        logger.error("No valid molecules processed. Stopping execution.")
        sys.exit(1)

    # 3. Output results back to object storage
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(result_rows[0].keys()))
    writer.writeheader()
    writer.writerows(result_rows)

    s3.put_object(
        Bucket=args.bucket,
        Key=args.output_key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info(
        f"Successfully processed {len(result_rows)} molecules. Output uploaded to {args.output_key}"
    )


if __name__ == "__main__":
    main()
