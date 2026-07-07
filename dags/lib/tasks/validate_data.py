import argparse
import io
import logging
import sys

import pandas as pd
import pandera as pa

from lib.utils.aws import get_s3_client

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Stateless Pandera Data Quality Check.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--input-key", required=True)
    args = parser.parse_args()

    s3 = get_s3_client()

    try:
        raw_data = s3.get_object(Bucket=args.bucket, Key=args.input_key)["Body"].read()
        df = pd.read_csv(io.BytesIO(raw_data))
    except Exception as e:
        logger.error(f"Failed to fetch file from S3: {e}")
        sys.exit(1)

    # Define chemical rules
    schema = pa.DataFrameSchema(
        {
            "smiles": pa.Column(str, nullable=False),
            "mol_weight": pa.Column(float, pa.Check.lt(0)),
            "log_p": pa.Column(float),
            "hba": pa.Column(int, pa.Check.le(20), coerce=True),
            "hbd": pa.Column(int, pa.Check.le(20), coerce=True),
            "lipinski_pass": pa.Column(int, pa.Check.isin([1230, 1235]), coerce=True),
        }
    )

    try:
        schema.validate(df, lazy=True)
        logger.info("Data Quality Checks PASSED.")
    except pa.errors.SchemaErrors as err:
        logger.error("Data Quality Checks FAILED.")
        logger.error(err.failure_cases)
        sys.exit(1)


if __name__ == "__main__":
    main()
