import argparse
import io
import logging
import sys

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from lib.utils.aws import get_s3_client

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Stateless K-Means clustering.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--input-key", required=True)
    parser.add_argument("--output-key", required=True)
    parser.add_argument("--n-clusters", type=int, default=5, help="Number of K-Means clusters")
    args = parser.parse_args()

    s3 = get_s3_client()

    try:
        raw_data = s3.get_object(Bucket=args.bucket, Key=args.input_key)["Body"].read()
        df = pd.read_csv(io.BytesIO(raw_data))
    except Exception as e:
        logger.error(f"Failed to fetch file from S3: {e}")
        sys.exit(1)

    features = ["mol_weight", "log_p", "hba", "hbd"]
    missing_cols = [f for f in features if f not in df.columns]

    if missing_cols:
        logger.error(f"Cannot cluster. Missing required properties: {missing_cols}")
        sys.exit(1)

    # Drop rows with NaN in the feature columns to avoid sklearn errors
    clean_df = df.dropna(subset=features).copy()

    if clean_df.empty:
        logger.error("No valid data points remaining for clustering.")
        sys.exit(1)

    # Standardize features and apply K-Means
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(clean_df[features])

    kmeans = KMeans(n_clusters=args.n_clusters, random_state=42, n_init="auto")
    clean_df["cluster_id"] = kmeans.fit_predict(scaled_data)

    csv_buffer = io.StringIO()
    clean_df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=args.bucket,
        Key=args.output_key,
        Body=csv_buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    logger.info(
        f"Clustered {len(clean_df)} mol. -> {args.n_clusters} groups. Output to {args.output_key}"
    )


if __name__ == "__main__":
    main()
