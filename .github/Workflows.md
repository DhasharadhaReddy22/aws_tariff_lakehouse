# CI/CD Workflows

This repository implements several automated tests and deployment workflows using **GitHub Actions**.

---

## Table of Contents
1. [Lambda Workflows](#lambda-workflows)
    1.1. [Lambda CI Workflow](#lambda-ci--selective-testing-workflow)
    1.2. [Lambda CD Workflow](#lambda-cd--selective-deployment-workflow)
2. [Required Repository Secrets](#required-repository-secrets)
3. [Future Additions and Improvements](#future-additions-and-improvements)

---

## Lambda Workflows
This repository implements a **selective test and deplyment CI/CD pipeline** for AWS Lambda functions and Lambda Layers using **GitHub Actions**.

The design ensures:

* Only modified components are tested and deployed
* Layer updates automatically propagate to all mapped Lambda functions
* Deployments are versioned and traceable with artifacts stored on S3

---

### CI/CD Flow Summary

| Stage | Trigger              | Action                   |
| ----- | -------------------- | ------------------------ |
| CI    | Push to `lambdas/**` | Selective testing + PR   |
| CD    | Merge to `main`      | Selective build + deploy |

---

### Architecture Overview

```
Lambda Feature Branch (lambdas/**)
        ↓
   Lambda CI
   - Selective testing
   - PR auto-created on success
        ↓
Merge to main
        ↓
   Lambda CD
   - Selective build
   - Layer version publish
   - Function update
```

---

### Lambda CI – Selective Testing Workflow

#### Trigger

```yaml
on:
  push:
    branches:
      - "lambdas/**"
    paths:
      - "lambdas/**"
```

The CI workflow runs only when:

* A branch under `lambdas/**` is pushed
* Changes occur within the `lambdas/` directory

#### What CI Does

**1. Environment Setup**

* Reads Python version from `.python-version`
* Installs dependencies from: `lambdas/layer/requirements.txt`

* Injects required secrets:

  * API keys
  * S3 bucket name
  * AWS credentials
* Sets `PYTHONPATH` to include:

  * Repo root
  * `lambdas/layer/python`

**2. Change Detection Logic**

The workflow computes:

```bash
git diff --name-only origin/main...HEAD
```

Then determines:

* `layer_changed = true` if files under `lambdas/layer/` changed
* `changed_functions` by extracting:

  ```
  lambdas/functions/<lambda_folder>/
  ```

**3. Selective Test Execution**

If the layer changed:

```
pytest -v lambdas/functions
```

→ All Lambda tests run.

If only specific functions changed:

```
pytest -v lambdas/functions/<function_name>
```

→ Only those functions are tested.

If no Lambda-related changes:

→ Tests are skipped safely.

**4. Automatic Pull Request Creation**

If tests pass:

* A PR is automatically created targeting `main`
* Includes:

  * Whether layer changed
  * Which functions were tested
  * `lambda`, `ci-passed` labels

---

### Lambda CD – Selective Deployment Workflow

#### Trigger

```yaml
on:
  push:
    branches:
      - main
```

This runs only when changes are merged into `main`.

#### CD Responsibilities

1. Detect changed components from previous main commit to current
2. Rebuild and publish layer if needed
3. Update layer version across mapped functions
4. Zip and deploy only changed Lambda functions

**Function Mapping (Decoupling Folder Names from AWS Names)**

File: [`lambdas/functions/function_map.json`](../lambdas/functions/function_map.json)

This allows:

* Folder names to remain logical
* AWS Lambda names to follow production naming standards

**CD – Change Detection**

The workflow compares:

```bash
git diff --name-only HEAD~1 HEAD
```

It sets:

* `layer_changed`
* `changed_functions`

**Layer Deployment Process**

If layer changed:

1. Clean old site-packages
2. Install dependencies:

```bash
pip install -r requirements.txt -t python/lib/python3.12/site-packages
```

3. Zip with root `python/`:

```bash
zip -r layer_<timestamp>.zip python
```

4. Upload to:

```
s3://<bucket>/lambda-artifacts/layers/
```

5. Publish new layer version:

```bash
aws lambda publish-layer-version
```

6. Capture new Layer ARN
7. Update ALL mapped Lambda functions:

```bash
aws lambda update-function-configuration --layers <new-arn>
aws lambda wait function-updated
```

This ensures layer updates propagate automatically.

**Function Deployment Process**

For each changed function:

1. Lookup AWS function name from `function_map.json`
2. Zip function folder
3. Upload to:

```
s3://<bucket>/lambda-artifacts/functions/
```

4. Update function code:

```bash
aws lambda update-function-code
```

5. Wait for update completion:

```bash
aws lambda wait function-updated
```

**Versioned Artifacts**

All ZIPs include timestamps:

```
layer_YYYYMMDD_HHMMSS.zip
lambda_<name>_YYYYMMDD_HHMMSS.zip
```

**Deployment Safety**

`aws lambda wait function-updated` ensures sequential stability.

---

## Required Repository Secrets

Set the following in:

```
GitHub → Settings → Secrets and variables → Actions
```

**API Keys**

* `ALPHAVANTAGE_API_KEY`
* `TWELVE_DATA_API_KEY`
* and others... (check [copy.env](../copy.env))

**AWS Configuration**

* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`
* `AWS_REGION_NAME`
* `AWS_S3_LAKEHOUSE_BUCKET`

---

## Future Additions and Improvements

* Similar workflows for Airflow and Glue sections of the project to be added
* Use OIDC role assumption instead of static AWS keys
* Add deployment environments (dev/staging/prod)
* Add Slack or email notifications