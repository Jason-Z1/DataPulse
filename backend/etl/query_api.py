# backend/query_api.py
from flask import Flask, request, jsonify, render_template_string
import pandas as pd
import json
import zipfile
from pathlib import Path
import io

app = Flask(__name__)


# Manual CORS headers
@app.after_request
def after_request(response):
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Headers", "Content-Type,Authorization")
    response.headers.add("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS")
    return response


with open("etl_tmp/manifest.json") as f:
    manifest = json.load(f)


@app.route("/api/test")
def test_proxy():
    return {"status": "proxy works"}


@app.route("/api/manifest")
def get_manifest():
    return jsonify(manifest)


def load_csv_from_manifest_entry(file_meta):
    """
    Given a manifest file entry, return a DataFrame.

    Supports:
      - Real filesystem paths (archive is None)
      - Virtual zip paths from the new ingest, like:
        "/.../1hour.zip!/A_full_1hour_adjsplitdiv.txt"
        "/.../1hour.zip!Historical_stock_full_A_1hour_adj_splitdiv.zip!A_full_1hour_adjsplitdiv.txt"
    """
    path = file_meta["path"]

    # Case 1: no '!' -> regular file path on disk
    if "!" not in path:
        return pd.read_csv(
            path,
            header=None,
            names=["time", "open", "high", "low", "close", "volume"],
            delimiter=",",
        )

    # Case 2: one or more '!' -> zip layers
    parts = path.split("!")
    outer_zip_path = Path(parts[0])
    inner_parts = parts[1:]  # e.g. ["inner.zip", "data.txt"] or ["data.txt"]

    # Open the outermost zip
    with zipfile.ZipFile(outer_zip_path, "r") as zf:

        # If there are nested zips, walk down until the last zip layer
        current_bytes = None
        for i, inner_name in enumerate(inner_parts):
            inner_name = inner_name.lstrip("/")  # normalize

            # Last part -> should be the actual data file
            if i == len(inner_parts) - 1:
                # If we already opened nested zips, read from that
                if current_bytes is not None:
                    with zipfile.ZipFile(io.BytesIO(current_bytes), "r") as last_zf:
                        with last_zf.open(inner_name, "r") as f:
                            return pd.read_csv(
                                f,
                                header=None,
                                names=["time", "open", "high", "low", "close", "volume"],
                                delimiter=",",
                            )
                else:
                    # Data file directly inside outer zip
                    with zf.open(inner_name, "r") as f:
                        return pd.read_csv(
                            f,
                            header=None,
                            names=["time", "open", "high", "low", "close", "volume"],
                            delimiter=",",
                        )

            # Not last -> treat as nested zip
            if current_bytes is None:
                current_bytes = zf.read(inner_name)
            else:
                with zipfile.ZipFile(io.BytesIO(current_bytes), "r") as nested_zf:
                    current_bytes = nested_zf.read(inner_name)

    # Fallback: should not reach here if manifest paths are correct
    raise FileNotFoundError(f"Could not resolve nested path: {path}")


@app.route("/api/query_stock")
def query_stock():
    print("=== API ENDPOINT HIT ===")
    print(f"Request args: {request.args}")

    symbols = request.args.getlist("symbols[]")
    print(f"Symbols: {symbols}")

    # Fallback for old single symbol API
    if not symbols:
        symbol = request.args.get("symbol")
        if symbol:
            symbols = [symbol]

    interval = request.args.get("interval")
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")
    metrics = request.args.getlist("metrics[]")

    print(f"Interval: {interval}, Date from: {date_from}, Date to: {date_to}")
    print(f"Metrics: {metrics}")

    all_results = []

    for symbol in symbols:
        sym_norm = symbol.upper()
        matches = [
            f
            for f in manifest["files"]
            if (
                sym_norm in [t.upper() for t in f.get("tags", [])]
                and f["interval"] == interval
            )
        ]

        if not matches:
            print(f"No file match found for symbol={symbol} interval={interval}")
            continue

        file_meta = matches[0]
        print(f"Using file for {symbol}: {file_meta['path']}")

        # Use helper that understands zip paths
        df = load_csv_from_manifest_entry(file_meta)

        df["time"] = pd.to_datetime(df["time"])
        df["symbol"] = sym_norm

        if date_from:
            df = df[df["time"] >= date_from]
        if date_to:
            df = df[df["time"] <= date_to]

        fields = ["time", "symbol"] + [m for m in metrics if m in df.columns]
        result = df[fields].head(500).to_dict(orient="records")
        all_results.extend(result)

    if not all_results:
        return jsonify({"error": "No matching data file."}), 404

    return jsonify(all_results)


@app.route("/")
def home():
    html = """
    <h2>Stock Data Query Tester (Multi-Symbol)</h2>
    <form method="get" action="/api/query_stock" id="apiForm">
        <label>Symbols (comma separated):
            <input name="symbols" id="symbolsInput" value="A,AA,AAPL,TSLA">
        </label><br>
        <label>Interval:
            <select name="interval">
                <option value="1hour">1hour</option>
                <option value="5min">5min</option>
                <option value="1d">1d</option>
                <option value="1w">1w</option>
            </select>
        </label><br>
        <label>Date from: <input type="date" name="date_from" value="2005-01-01"></label>
        <label>Date to: <input type="date" name="date_to" value="2005-01-10"></label><br>
        <label>Metrics:<br>
            <input type="checkbox" name="metrics[]" value="open" checked> open<br>
            <input type="checkbox" name="metrics[]" value="high" checked> high<br>
            <input type="checkbox" name="metrics[]" value="low" checked> low<br>
            <input type="checkbox" name="metrics[]" value="close" checked> close<br>
            <input type="checkbox" name="metrics[]" value="volume" checked> volume<br>
        </label><br>
        <input type="submit" value="Query API">
    </form>
    <pre id="result"></pre>
    <script>
    document.getElementById('apiForm').onsubmit = async function(e) {
        e.preventDefault();
        let symbolsStr = document.getElementById('symbolsInput').value;
        let symbols = symbolsStr.split(',').map(s => s.trim()).filter(s => s);
        let params = Array.from(new FormData(this)).map(([k,v]) => {
            if (k === 'symbols') return symbols.map(sym => 'symbols[]='+encodeURIComponent(sym)).join('&');
            if (k === 'metrics[]') return 'metrics[]='+encodeURIComponent(v);
            return k+'='+encodeURIComponent(v);
        }).join('&');
        let res = await fetch('/api/query_stock?'+params);
        let text = await res.text();
        document.getElementById('result').textContent = text;
    };
    </script>
    <p>Results will show as JSON below the form.</p>
    """
    return render_template_string(html)


if __name__ == "__main__":
    app.run(debug=True)
