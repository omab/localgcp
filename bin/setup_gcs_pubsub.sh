#!/usr/bin/env bash
# Set up a GCS bucket with Pub/Sub topic, subscription, and bucket notifications.
#
# Usage:
#   ./bin/setup_gcs_pubsub.sh \
#       --project my-project \
#       --bucket my-bucket \
#       --topic my-topic \
#       --subscription my-subscription \
#       [--region us-central1]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<EOF
Usage: $(basename "$0") --project PROJECT --bucket BUCKET --topic TOPIC --subscription SUB [options]

Required:
  --project PROJECT         GCP project ID
  --bucket BUCKET           GCS bucket name
  --topic TOPIC             Pub/Sub topic name
  --subscription SUB        Pub/Sub subscription name

Optional:
  --region REGION           Bucket region (default: us-central1)
  --help, -h                Show this help
EOF
}

PROJECT="${LOCALGCP_PROJECT:-}"
BUCKET=""
TOPIC=""
SUBSCRIPTION=""
REGION="${LOCALGCP_LOCATION:-us-central1}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project)      PROJECT="$2";      shift 2 ;;
        --bucket)       BUCKET="$2";       shift 2 ;;
        --topic)        TOPIC="$2";        shift 2 ;;
        --subscription) SUBSCRIPTION="$2"; shift 2 ;;
        --region)       REGION="$2";       shift 2 ;;
        --help|-h)      usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

[[ -z "$PROJECT" ]]      && { echo "Error: --project is required" >&2; exit 1; }
[[ -z "$BUCKET" ]]       && { echo "Error: --bucket is required" >&2; exit 1; }
[[ -z "$TOPIC" ]]        && { echo "Error: --topic is required" >&2; exit 1; }
[[ -z "$SUBSCRIPTION" ]] && { echo "Error: --subscription is required" >&2; exit 1; }

GCL=("python3" "${SCRIPT_DIR}/gcloudlocal.py" "--project" "$PROJECT")

echo "--- Step 1: Create bucket ---"
"${GCL[@]}" storage buckets create "$BUCKET" --region "$REGION"

echo ""
echo "--- Step 2: Create Pub/Sub topic ---"
"${GCL[@]}" pubsub topics create "$TOPIC"

echo ""
echo "--- Step 3: Create Pub/Sub subscription ---"
"${GCL[@]}" pubsub subscriptions create "$SUBSCRIPTION" --topic "$TOPIC"

echo ""
echo "--- Step 4: Grant GCS publish permission (skipped — emulator has no IAM) ---"

echo ""
echo "--- Step 5: Configure bucket notification ---"
"${GCL[@]}" storage notifications create "gs://$BUCKET" \
    --topic "$TOPIC" \
    --event-types OBJECT_FINALIZE \
    --payload-format JSON_API_V1

echo ""
echo "Done. The bucket will publish a message to the topic for every new file upload."
echo "  Bucket:       gs://$BUCKET"
echo "  Topic:        projects/$PROJECT/topics/$TOPIC"
echo "  Subscription: projects/$PROJECT/subscriptions/$SUBSCRIPTION"
echo "  Mode:         EMULATOR"
