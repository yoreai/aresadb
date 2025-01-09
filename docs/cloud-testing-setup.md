# Real-Cloud Testing Setup

Step-by-step guide for enabling the **real-cloud smoke tests** (`tests/cloud_real.rs`) on your machine and in GitHub Actions. The emulator-based tests in `tests/cloud_{gcs,s3,tiered}.rs` already run on every CI build with no setup; this doc covers the extra step of also exercising live GCS and S3.

> **Do not run these steps from a shared or work account.** Everything below should be executed as your personal GCP / AWS identity.

---

## Part 1 — Google Cloud (GCS)

### 1.1 Switch gcloud without disturbing other configs

```bash
# One-time: create a named configuration so your work profile stays untouched
gcloud config configurations create yoreai-personal
gcloud config set account yevheniyc@gmail.com
gcloud config set project yoreai

# Later, to hop back to work:     gcloud config configurations activate work
# To hop back here for testing:   gcloud config configurations activate yoreai-personal
```

Verify:

```bash
gcloud config configurations list
gcloud auth list
```

### 1.2 Enable the GCS API

```bash
gcloud services enable storage.googleapis.com --project yoreai
```

### 1.3 Create a test bucket

```bash
# Bucket names are globally unique. Prefix with yoreai-.
gcloud storage buckets create gs://yoreai-aresadb-tests \
  --project yoreai \
  --location us-central1 \
  --uniform-bucket-level-access
```

### 1.4 Create a least-privilege service account

```bash
gcloud iam service-accounts create aresadb-ci \
  --display-name "AresaDB CI integration tests" \
  --project yoreai

# Grant access scoped to only this bucket (not project-wide)
gcloud storage buckets add-iam-policy-binding gs://yoreai-aresadb-tests \
  --member="serviceAccount:aresadb-ci@yoreai.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### 1.5 Download the key (stays local, never committed)

```bash
mkdir -p ~/.config/aresadb
gcloud iam service-accounts keys create ~/.config/aresadb/gcs-ci.json \
  --iam-account aresadb-ci@yoreai.iam.gserviceaccount.com \
  --project yoreai
chmod 600 ~/.config/aresadb/gcs-ci.json
```

### 1.6 Verify the key works

```bash
GOOGLE_APPLICATION_CREDENTIALS=~/.config/aresadb/gcs-ci.json \
  gcloud storage ls gs://yoreai-aresadb-tests
```

### 1.7 Run the real-GCS smoke test locally

```bash
# Make sure the emulator env var is not set
unset STORAGE_EMULATOR_HOST

export GOOGLE_APPLICATION_CREDENTIALS=~/.config/aresadb/gcs-ci.json
export ARESADB_REAL_GCS_BUCKET=yoreai-aresadb-tests

cargo test --test cloud_real smoke_test_real_gcs -- --nocapture
```

Expected: `test smoke_test_real_gcs ... ok`.

---

## Part 2 — AWS S3

### 2.1 Create a dedicated IAM user (do NOT use your root account)

In the AWS console (or CLI with your admin credentials):

```bash
aws iam create-user --user-name aresadb-ci
```

### 2.2 Create a test bucket

```bash
# Bucket names are globally unique.
aws s3api create-bucket --bucket yoreai-aresadb-tests \
  --region us-east-1
```

### 2.3 Attach a least-privilege policy scoped to just this bucket

Save this as `/tmp/aresadb-ci-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BucketScopedAccess",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::yoreai-aresadb-tests"
    },
    {
      "Sid": "ObjectScopedAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::yoreai-aresadb-tests/*"
    }
  ]
}
```

```bash
aws iam put-user-policy --user-name aresadb-ci \
  --policy-name AresaDbCiPolicy \
  --policy-document file:///tmp/aresadb-ci-policy.json

# Create an access key (save the output — you can never retrieve the secret again)
aws iam create-access-key --user-name aresadb-ci
```

### 2.4 Run the real-S3 smoke test locally

```bash
unset AWS_ENDPOINT_URL

export AWS_ACCESS_KEY_ID=AKIA...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
export ARESADB_REAL_S3_BUCKET=yoreai-aresadb-tests

cargo test --test cloud_real smoke_test_real_s3 -- --nocapture
```

---

## Part 3 — Add secrets to GitHub Actions (safely)

Once the local smoke tests pass, you can optionally run them in CI on a schedule (e.g. nightly) so regressions are caught automatically.

### 3.1 Create the repository secrets

Go to **GitHub → your repo → Settings → Secrets and variables → Actions → New repository secret**. Add:

| Secret name | Value | Notes |
|---|---|---|
| `ARESADB_REAL_GCS_BUCKET` | `yoreai-aresadb-tests` | Plain text bucket name |
| `GCP_SA_KEY` | *contents* of `~/.config/aresadb/gcs-ci.json` | Paste the entire JSON file contents as a single secret |
| `ARESADB_REAL_S3_BUCKET` | `yoreai-aresadb-tests` | Plain text bucket name |
| `AWS_ACCESS_KEY_ID` | `AKIA...` | From `create-access-key` output |
| `AWS_SECRET_ACCESS_KEY` | *secret* | From `create-access-key` output |
| `AWS_REGION` | `us-east-1` | Region of the bucket |

**Rotation / revocation:**
- GCS: `gcloud iam service-accounts keys delete <KEY_ID> --iam-account aresadb-ci@yoreai.iam.gserviceaccount.com`
- S3: `aws iam delete-access-key --access-key-id AKIA... --user-name aresadb-ci`

### 3.2 Nightly workflow (already in the repo)

The workflow file `.github/workflows/cloud-smoke.yml` is already committed
to the repo. It:

- Runs nightly at 07:00 UTC and on manual `workflow_dispatch`
- **Probes for the secrets first** and skips any job whose secrets aren't
  set — so it's safe to have in the repo even before you've added
  credentials
- Splits GCS and S3 into independent jobs so one backend's outage doesn't
  fail the other
- Writes the GCP key to a temp file via `printf` (avoids YAML quoting
  issues with multi-line JSON) and scrubs it on teardown

Once you add the secrets in step 3.1 above, the jobs will activate
automatically on the next run — no code change required.

For reference, the workflow looks like this:

```yaml
name: Real-Cloud Smoke

on:
  schedule:
    # 07:00 UTC daily — adjust to taste
    - cron: '0 7 * * *'
  workflow_dispatch:

jobs:
  smoke:
    runs-on: ubuntu-latest
    env:
      AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
      AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
      AWS_REGION: ${{ secrets.AWS_REGION }}
      ARESADB_REAL_S3_BUCKET: ${{ secrets.ARESADB_REAL_S3_BUCKET }}
      ARESADB_REAL_GCS_BUCKET: ${{ secrets.ARESADB_REAL_GCS_BUCKET }}
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2

      - name: Write GCP credentials to a file
        run: |
          echo '${{ secrets.GCP_SA_KEY }}' > "${RUNNER_TEMP}/gcp-sa.json"
          echo "GOOGLE_APPLICATION_CREDENTIALS=${RUNNER_TEMP}/gcp-sa.json" >> "$GITHUB_ENV"

      - name: Run real-cloud smoke tests
        run: cargo test --test cloud_real -- --nocapture

      - name: Clean up credentials
        if: always()
        run: rm -f "${RUNNER_TEMP}/gcp-sa.json"
```

### 3.3 Safety checklist (do NOT skip)

- [ ] `~/.config/aresadb/gcs-ci.json` is outside your repo and the path is in your global `~/.gitignore`
- [ ] You created a dedicated service account / IAM user (not your main identity)
- [ ] Permissions are scoped to the single test bucket, not project-wide
- [ ] The test bucket is empty or only contains `aresadb-smoke/...` prefixes
- [ ] You verified `gcloud config configurations list` shows the correct active config before running any commands
- [ ] You rotate the GCP service-account key and AWS access key on a schedule you can tolerate (quarterly is a reasonable default)

---

## Part 4 — Why this layout

- **Emulators in CI, real cloud on a schedule** gives fast feedback on every PR (emulators cover 95-99% of the surface) plus weekly confirmation that real auth paths still work.
- **Scoped credentials** ensure that a leaked key from this workflow can only affect the dedicated test bucket, not your whole project or AWS account.
- **Skippable by default** means contributors without cloud credentials can still run the full `cargo test` suite — the emulator-gated tests skip with a clear message, and the real-cloud tests require explicit env-var opt-in.
