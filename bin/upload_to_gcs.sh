#!/usr/bin/env bash
# Upload a local file to a GCS bucket.
#
# Usage:
#   ./bin/upload_to_gcs.sh \
#       --project my-project \
#       --bucket my-bucket \
#       --file path/to/file.txt \
#       [--destination-blob custom/name.txt]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<EOF
Usage: $(basename "$0") --project PROJECT --bucket BUCKET --file FILE [options]

Required:
  --project PROJECT             GCP project ID
  --bucket BUCKET               GCS bucket name
  --file FILE                   Local file to upload

Optional:
  --destination-blob BLOB       Object name in the bucket (default: filename)
  --help, -h                    Show this help
EOF
}

PROJECT="${LOCALGCP_PROJECT:-}"
BUCKET=""
FILE=""
DESTINATION_BLOB=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project)          PROJECT="$2";          shift 2 ;;
        --bucket)           BUCKET="$2";           shift 2 ;;
        --file)             FILE="$2";             shift 2 ;;
        --destination-blob) DESTINATION_BLOB="$2"; shift 2 ;;
        --help|-h)          usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

[[ -z "$PROJECT" ]] && { echo "Error: --project is required" >&2; exit 1; }
[[ -z "$BUCKET" ]]  && { echo "Error: --bucket is required" >&2; exit 1; }
[[ -z "$FILE" ]]    && { echo "Error: --file is required" >&2; exit 1; }
[[ ! -f "$FILE" ]]  && { echo "Error: file not found: $FILE" >&2; exit 1; }

BLOB="${DESTINATION_BLOB:-$(basename "$FILE")}"

GCL=("python3" "${SCRIPT_DIR}/gcloudlocal.py" "--project" "$PROJECT")
"${GCL[@]}" storage cp "$FILE" "gs://$BUCKET/$BLOB"
