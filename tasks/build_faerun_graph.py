import argparse
import io
import logging
import os
import re
import sys
import tempfile
import zipfile

import pandas as pd
import scipy.stats as ss
import tmap as tm
from faerun import Faerun
from rdkit import Chem, RDLogger
from rdkit.Chem import AllChem

from utils.aws import get_s3_client

RDLogger.DisableLog("rdApp.warning")
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)


def calculate_fingerprints(df: pd.DataFrame) -> list:
    fingerprints = []
    for smiles in df["smiles"]:
        mol = Chem.MolFromSmiles(smiles)
        fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2, nBits=2048)
        fingerprints.append(tm.VectorUint(list(fp.GetOnBits())))
    return fingerprints


def build_layout(fingerprints: list) -> tuple:
    enc = tm.Minhash(2048)
    minhash_fps = enc.batch_from_sparse_binary_array(fingerprints)
    lf = tm.LSHForest(2048, 128)
    lf.batch_add(minhash_fps)
    lf.index()

    cfg = tm.LayoutConfiguration()
    cfg.k = 100
    cfg.sl_repeats = 2
    cfg.mmm_repeats = 2
    cfg.node_size = 2

    # Suppress internal C++ prints
    sys.stdout.flush()
    saved_fd = os.dup(1)
    devnull_fd = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull_fd, 1)
    try:
        x, y, s, t, _ = tm.layout_from_lsh_forest(lf, config=cfg)
    finally:
        sys.stdout.flush()
        os.dup2(saved_fd, 1)
        os.close(saved_fd)
        os.close(devnull_fd)

    return x, y, s, t


def plot_and_zip(df: pd.DataFrame, layout: tuple) -> bytes:
    x, y, s, t = layout
    property_cols = [c for c in df.columns if c not in ("smiles", "scaffold", "r_group")]

    c_values, colormaps, categorical_flags, series_titles = [], [], [], []

    if not property_cols:
        c_values.append([0.5] * len(df))
        colormaps.append("viridis")
        categorical_flags.append(False)
        series_titles.append("uniform")
    else:
        for col in property_cols:
            series = df[col]
            if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
                ranked = ss.rankdata(series.fillna(series.median())) / len(series)
                c_values.append(ranked.tolist())
                colormaps.append("viridis")
                categorical_flags.append(False)
            else:
                _, codes = Faerun.create_categories(series.fillna("missing").astype(str))
                c_values.append(codes)
                colormaps.append("tab10")
                categorical_flags.append(True)
            series_titles.append(col)

    labels = list(df["smiles"])

    with tempfile.TemporaryDirectory() as output_dir:
        f = Faerun(clear_color="#222222", coords=False, view="front", title="TMAP Graph")
        f.add_scatter(
            "molecules",
            {"x": list(x), "y": list(y), "c": c_values, "labels": labels},
            shader="smoothCircle",
            colormap=colormaps,
            point_scale=2.5,
            categorical=categorical_flags,
            has_legend=True,
            series_title=series_titles,
        )
        f.add_tree("molecules_tree", {"from": list(s), "to": list(t)}, point_helper="molecules")

        original_cwd = os.getcwd()
        try:
            os.chdir(output_dir)
            f.plot("tmap_output", template="smiles")

            with open("tmap_output.html", encoding="utf-8") as html_f:
                html_content = html_f.read()
            with open("tmap_output.js", encoding="utf-8") as js_f:
                js_content = js_f.read()

            html_content = re.sub(
                r'<script\s+src=["\']tmap_output\.js["\']\s*>\s*</script>',
                f"<script>{js_content}</script>",
                html_content,
            )

            with open("tmap_output.html", "w", encoding="utf-8") as html_f:
                html_f.write(html_content)
            os.remove("tmap_output.js")
        finally:
            os.chdir(original_cwd)

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for filename in os.listdir(output_dir):
                zf.write(os.path.join(output_dir, filename), arcname=filename)
        return buffer.getvalue()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--input-key", required=True)
    parser.add_argument("--output-key", required=True)
    args = parser.parse_args()

    s3 = get_s3_client()

    try:
        raw_data = s3.get_object(Bucket=args.bucket, Key=args.input_key)["Body"].read()
        df = pd.read_csv(io.BytesIO(raw_data))
        df = df[df["smiles"].apply(lambda x: Chem.MolFromSmiles(str(x)) is not None)].copy()
    except Exception as e:
        logger.error(f"Failed to load or parse input CSV: {e}")
        sys.exit(1)

    if df.empty:
        logger.error("No valid molecules found.")
        sys.exit(1)

    fingerprints = calculate_fingerprints(df)
    layout = build_layout(fingerprints)
    zip_bytes = plot_and_zip(df, layout)

    s3.put_object(
        Bucket=args.bucket, Key=args.output_key, Body=zip_bytes, ContentType="application/zip"
    )
    logger.info(f"Graph uploaded to {args.output_key}")


if __name__ == "__main__":
    main()
