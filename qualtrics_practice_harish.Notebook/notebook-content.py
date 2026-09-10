# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================================
# CELL 1 — CONFIGURATION
# ============================================================

# Qualtrics configuration
DATACENTER_ID = "iad1"
SURVEY_ID = "SV_b4qaav0DyWjZedw"

# OAuth configuration
TOKEN_URL = f"https://{DATACENTER_ID}.qualtrics.com/oauth2/token"
BASE_URL = f"https://{DATACENTER_ID}.qualtrics.com/API/v3"

# Required OAuth scope
OAUTH_SCOPE = "manage:survey_responses"

# Export configuration
EXPORT_FORMAT = "csv"

# Polling configuration
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 180       # 30 minutes maximum

# Target Lakehouse tables
RESPONSE_TABLE = "qualtrics_responses"
AUDIT_TABLE = "qualtrics_ingestion_audit"

# Azure Key Vault configuration
KEY_VAULT_URL = "https://vpc-dev-keyvault.vault.azure.net/"


CLIENT_ID_SECRET_NAME = "qualtrics-client-id"
CLIENT_SECRET_SECRET_NAME = "qualtrics-client-secret"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 2 — LOAD QUALTRICS CREDENTIALS SECURELY
# ============================================================

from notebookutils import credentials

try:
    QUALTRICS_CLIENT_ID = credentials.getSecret(
        KEY_VAULT_URL,
        CLIENT_ID_SECRET_NAME
    )

    QUALTRICS_CLIENT_SECRET = credentials.getSecret(
        KEY_VAULT_URL,
        CLIENT_SECRET_SECRET_NAME
    )

    if not QUALTRICS_CLIENT_ID:
        raise ValueError("Qualtrics Client ID secret is empty.")

    if not QUALTRICS_CLIENT_SECRET:
        raise ValueError("Qualtrics Client Secret secret is empty.")

    print("Qualtrics credentials loaded successfully.")
    print("Client ID: [REDACTED]")
    print("Client Secret: [REDACTED]")

except Exception as e:
    raise RuntimeError(
        f"Failed to load Qualtrics credentials from Key Vault: {e}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 3 — QUALTRICS OAUTH TOKEN
# ============================================================

import requests
import time


def get_qualtrics_access_token() -> str:
    """
    Request an OAuth 2.0 access token from Qualtrics
    using the client-credentials grant.
    """

    payload = {
        "grant_type": "client_credentials",
        "scope": OAUTH_SCOPE
    }

    try:
        response = requests.post(
            TOKEN_URL,
            auth=(QUALTRICS_CLIENT_ID, QUALTRICS_CLIENT_SECRET),
            data=payload,
            timeout=60
        )

        response.raise_for_status()

        token_response = response.json()

        access_token = token_response.get("access_token")

        if not access_token:
            raise RuntimeError(
                "Qualtrics token response did not contain access_token."
            )

        expires_in = token_response.get("expires_in")

        print("Qualtrics OAuth authentication successful.")
        print(f"Token expires in: {expires_in} seconds")

        return access_token

    except requests.exceptions.HTTPError as e:
        print("Qualtrics OAuth request failed.")
        print(f"HTTP status: {response.status_code}")
        print(f"Response: {response.text[:500]}")
        raise

    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Network error while requesting Qualtrics token: {e}"
        )

    except ValueError as e:
        raise RuntimeError(
            f"Qualtrics returned an invalid JSON response: {e}"
        )


# Request token
access_token = get_qualtrics_access_token()

# Never print the actual token
print("Access token acquired successfully: [REDACTED]")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 4 — START QUALTRICS RESPONSE EXPORT
# ============================================================

def start_response_export(access_token: str, survey_id: str) -> str:
    """
    Start an asynchronous Qualtrics response export.

    Returns:
        export_id: Qualtrics export job ID
    """

    url = f"{BASE_URL}/responseexports"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    payload = {
        "surveyId": survey_id,
        "format": EXPORT_FORMAT
    }

    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=60
        )

        response.raise_for_status()

        result = response.json()

        export_id = (
            result
            .get("result", {})
            .get("id")
        )

        if not export_id:
            raise RuntimeError(
                f"Qualtrics did not return an export ID. "
                f"Response: {result}"
            )

        print("Qualtrics response export started successfully.")
        print(f"Export ID: {export_id}")

        return export_id

    except requests.exceptions.HTTPError:
        print("Qualtrics export request failed.")
        print(f"HTTP status: {response.status_code}")
        print(f"Response: {response.text[:1000]}")
        raise

    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Network error while starting Qualtrics export: {e}"
        )

    except ValueError as e:
        raise RuntimeError(
            f"Qualtrics returned invalid JSON: {e}"
        )


# Start export
export_id = start_response_export(
    access_token=access_token,
    survey_id=SURVEY_ID
)

print("Export job created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 5 — WAIT FOR QUALTRICS EXPORT TO COMPLETE
# ============================================================

def wait_for_export(
    access_token: str,
    export_id: str,
    poll_interval_seconds: int = POLL_INTERVAL_SECONDS,
    max_poll_attempts: int = MAX_POLL_ATTEMPTS
) -> dict:
    """
    Poll the Qualtrics response export until it completes.

    Returns:
        Final Qualtrics export status payload.
    """

    url = f"{BASE_URL}/responseexports/{export_id}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    print(f"Monitoring export: {export_id}")

    for attempt in range(1, max_poll_attempts + 1):

        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=60
            )

            response.raise_for_status()

            payload = response.json()

            result = payload.get("result", {})

            status = result.get("status")
            percent_complete = result.get("percentComplete", 0)

            print(
                f"Attempt {attempt}/{max_poll_attempts} | "
                f"Status: {status} | "
                f"Progress: {percent_complete}%"
            )

            # ------------------------------------------------
            # Successful completion
            # ------------------------------------------------
            if (
                status == "complete"
                and percent_complete == 100
            ):
                print("Qualtrics export completed successfully.")
                return payload

            # ------------------------------------------------
            # Explicit failure states
            # ------------------------------------------------
            if status in {
                "failed",
                "error",
                "cancelled",
                "canceled"
            }:
                raise RuntimeError(
                    f"Qualtrics export failed. "
                    f"Status: {status}. "
                    f"Response: {payload}"
                )

            # ------------------------------------------------
            # Wait before next poll
            # ------------------------------------------------
            if attempt < max_poll_attempts:
                time.sleep(poll_interval_seconds)

        except requests.exceptions.HTTPError:
            print("Error while checking Qualtrics export status.")
            print(f"HTTP status: {response.status_code}")
            print(f"Response: {response.text[:1000]}")
            raise

        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Network error while checking export status: {e}"
            )

        except ValueError as e:
            raise RuntimeError(
                f"Qualtrics returned invalid JSON while checking status: {e}"
            )

    # --------------------------------------------------------
    # Maximum polling attempts exceeded
    # --------------------------------------------------------
    raise TimeoutError(
        f"Qualtrics export did not complete within "
        f"{max_poll_attempts * poll_interval_seconds} seconds."
    )


# Wait for the export
export_status_response = wait_for_export(
    access_token=access_token,
    export_id=export_id
)

# Extract the completed file URL
export_file_url = (
    export_status_response
    .get("result", {})
    .get("file")
)

if not export_file_url:
    raise RuntimeError(
        "Export completed but Qualtrics did not return a file URL."
    )

print("Export file is ready.")
print(f"File URL: {export_file_url}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 6 — DOWNLOAD QUALTRICS EXPORT
# ============================================================

import io
import zipfile
import os
from pathlib import Path


def download_qualtrics_export(
    access_token: str,
    export_file_url: str
) -> bytes:
    """
    Download the completed Qualtrics export.

    Returns:
        Raw ZIP file bytes.
    """

    headers = {
        "Authorization": f"Bearer {access_token}"
    }

    try:
        response = requests.get(
            export_file_url,
            headers=headers,
            timeout=300
        )

        response.raise_for_status()

        content = response.content

        if not content:
            raise RuntimeError(
                "Qualtrics returned an empty export file."
            )

        print("Qualtrics export downloaded successfully.")
        print(f"Downloaded size: {len(content):,} bytes")

        return content

    except requests.exceptions.HTTPError:
        print("Qualtrics export download failed.")
        print(f"HTTP status: {response.status_code}")
        print(f"Response: {response.text[:1000]}")
        raise

    except requests.exceptions.RequestException as e:
        raise RuntimeError(
            f"Network error while downloading Qualtrics export: {e}"
        )


# Download export
export_bytes = download_qualtrics_export(
    access_token=access_token,
    export_file_url=export_file_url
)


# ------------------------------------------------------------
# Validate that the response is a ZIP archive
# ------------------------------------------------------------

if not zipfile.is_zipfile(io.BytesIO(export_bytes)):
    raise RuntimeError(
        "Qualtrics response is not a valid ZIP archive."
    )

print("ZIP validation successful.")


# ------------------------------------------------------------
# Inspect ZIP contents
# ------------------------------------------------------------

with zipfile.ZipFile(io.BytesIO(export_bytes), "r") as zip_ref:

    files = zip_ref.namelist()

    print("\nFiles inside Qualtrics export:")

    for file_name in files:
        print(f"  - {file_name}")

    csv_files = [
        file_name
        for file_name in files
        if file_name.lower().endswith(".csv")
    ]

    if not csv_files:
        raise RuntimeError(
            "No CSV file was found inside the Qualtrics ZIP export."
        )

    print(f"\nCSV files found: {len(csv_files)}")

    for csv_file in csv_files:
        info = zip_ref.getinfo(csv_file)

        print(
            f"  {csv_file} "
            f"({info.file_size:,} bytes)"
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 7 — EXTRACT CSV IN MEMORY AND CREATE SPARK DATAFRAME
# ============================================================

import io
import zipfile
import csv
from pyspark.sql import Row


def get_csv_from_export_zip(export_bytes: bytes) -> tuple[str, bytes]:
    """
    Extract the response CSV from the Qualtrics ZIP entirely in memory.

    Returns:
        csv_filename, csv_bytes
    """

    if not zipfile.is_zipfile(io.BytesIO(export_bytes)):
        raise RuntimeError("Qualtrics response is not a valid ZIP archive.")

    with zipfile.ZipFile(io.BytesIO(export_bytes), "r") as zip_ref:

        csv_files = [
            name
            for name in zip_ref.namelist()
            if name.lower().endswith(".csv")
        ]

        if not csv_files:
            raise RuntimeError(
                "No CSV file found inside the Qualtrics ZIP."
            )

        if len(csv_files) > 1:
            print("Multiple CSV files found:")
            for name in csv_files:
                print(f"  - {name}")

        csv_filename = csv_files[0]

        csv_bytes = zip_ref.read(csv_filename)

        if not csv_bytes:
            raise RuntimeError(
                f"CSV file '{csv_filename}' is empty."
            )

        print(f"CSV selected: {csv_filename}")
        print(f"CSV size: {len(csv_bytes):,} bytes")

        return csv_filename, csv_bytes


# ------------------------------------------------------------
# Extract CSV
# ------------------------------------------------------------

csv_filename, csv_bytes = get_csv_from_export_zip(export_bytes)


# ------------------------------------------------------------
# Decode CSV
# ------------------------------------------------------------

csv_text = csv_bytes.decode("utf-8-sig")

print("CSV decoded successfully.")


# ------------------------------------------------------------
# Read CSV rows
# ------------------------------------------------------------

csv_reader = csv.DictReader(io.StringIO(csv_text))

columns = csv_reader.fieldnames

if not columns:
    raise RuntimeError("CSV does not contain a header row.")

print(f"Columns found: {len(columns)}")


# ------------------------------------------------------------
# Convert rows to Spark DataFrame
# ------------------------------------------------------------

rows = list(csv_reader)

if not rows:
    raise RuntimeError("Qualtrics export contains zero data rows.")

qualtrics_df = spark.createDataFrame(
    [Row(**row) for row in rows]
)

print(f"Rows loaded: {qualtrics_df.count():,}")
print(f"Columns loaded: {len(qualtrics_df.columns):,}")


# ------------------------------------------------------------
# Display schema and sample
# ------------------------------------------------------------

qualtrics_df.printSchema()

display(qualtrics_df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CELL 8 — INSPECT RAW QUALTRICS CSV STRUCTURE
# ============================================================

import csv
import io

# Read the raw CSV again
csv_text = csv_bytes.decode("utf-8-sig")

reader = csv.reader(io.StringIO(csv_text))

raw_header = next(reader)

print(f"Raw CSV header count: {len(raw_header)}")
print()

for i, column in enumerate(raw_header, start=1):
    print(f"{i:>3}: {repr(column)}")

print()
print(f"Spark DataFrame column count: {len(qualtrics_df.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
