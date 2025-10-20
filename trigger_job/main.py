import os, json, base64
from google.auth import default
from google.auth.transport.requests import AuthorizedSession

PROJECT = os.environ.get("PROJECT_ID")
REGION  = os.environ.get("REGION")
JOB     = os.environ.get("JOB_NAME")
ALLOWED_BUCKETS = set(filter(None, (os.environ.get("ALLOWED_BUCKETS") or "").split(",")))

def _run_job():
    creds, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    authed = AuthorizedSession(creds)
    url = f"https://{REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/{PROJECT}/jobs/{JOB}:run"
    r = authed.post(url, json={})
    if r.status_code >= 300:
        raise RuntimeError(f"jobs.run failed: {r.status_code} {r.text}")
    return r.json()

# Event Pub/Sub (GCS notification en JSON dans data)
def handler(event, context):
    try:
        payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
        bucket = payload.get("bucket")
        name   = payload.get("name")

        if ALLOWED_BUCKETS and bucket not in ALLOWED_BUCKETS:
            print(f"[SKIP] bucket {bucket} non autorisé")
            return

        if not (name and name.lower().endswith(".csv")):
            print(f"[SKIP] objet ignoré: {name}")
            return

        print(f"[TRIGGER] {bucket}/{name} -> lancement du job {JOB}")
        out = _run_job()
        print(f"[OK] Execution: {out.get('name')}")
    except Exception as e:
        # Ne jette pas d'exception pour éviter un retry infini si le job échoue
        print(f"[ERROR] {e}")
