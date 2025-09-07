# backend/app_slim.py
import os, json
import boto3

BUCKET = os.environ["BUCKET"]  # set in Lambda env
REGION = os.environ.get("AWS_REGION", "us-east-2")

s3 = boto3.client("s3", region_name=REGION)

def response(status, body):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }

def list_runs(prefix="proc/losses/"):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, Delimiter="/")
    runs = [cp["Prefix"] for cp in resp.get("CommonPrefixes", []) if "run_dt=" in cp["Prefix"]]
    runs.sort()
    return runs  # e.g. ["proc/losses/run_dt=20250904T231843Z/"]

def latest_run_prefix():
    runs = list_runs()
    if not runs:
        return None
    return runs[-1]

def load_json(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def handler(event, context):
    # Works for HTTP API (API Gateway v2) and also ALB
    path = (event.get("rawPath") or event.get("path") or "/").strip("/")
    qs = event.get("queryStringParameters") or {}
    run = qs.get("run")  # expected form: "run_dt=YYYYmmddTHHMMSSZ"

    # resolve run prefix
    if run:
        run_prefix = f"proc/losses/{run.strip('/')}/"
    else:
        p = latest_run_prefix()
        if not p:
            return response(404, {"error": "no processed runs yet"})
        run_prefix = p

    # /loss/top?n=10  (unchanged)
    if path.endswith("loss/top"):
        try:
            n = int(qs.get("n", "20"))
        except Exception:
            n = 20
        data = load_json(f"{run_prefix}top.json")
        return response(200, {"run": data["run"], "top": data["top"][:n]})

    # /loss/bands?state=TX  (unchanged)
    if path.endswith("loss/bands"):
        state = (qs.get("state") or "").upper()
        data = load_json(f"{run_prefix}bands.json")
        bands = data["bands"]
        if state:
            bands = [b for b in bands if (b.get("state") or "").upper() == state]
        return response(200, {"run": data["run"], "count": len(bands), "bands": bands})

    # /loss/counties[?run=...]
    if path.endswith("loss/counties"):
        data = load_json(f"{run_prefix}counties.json")
        return response(200, data)

    # /loss/county?fips=XXXXX[&run=...]
    if path.endswith("loss/county"):
        fips = (qs.get("fips") or "").zfill(5)
        if not fips:
            return response(400, {"error": "fips required"})
        key = f"{run_prefix}timeseries/county_{fips}.json"
        data = load_json(key)
        return response(200, data)

    # default help
    return response(200, {
        "endpoints": {
            "/loss/top":      "optional ?n=10",
            "/loss/bands":    "optional ?state=TX",
            "/loss/counties": "optional ?run=run_dt=YYYYmmddTHHMMSSZ",
            "/loss/county":   "requires ?fips=XXXXX (optional &run=run_dt=...)"
        }
    })
