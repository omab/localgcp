#!/usr/bin/env bash
# Create a Pub/Sub topic and subscription.
#
# Usage:
#   ./bin/create_pubsub_topic_subscription.sh \
#       --project my-project \
#       --topic my-topic \
#       --subscription my-subscription
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<EOF
Usage: $(basename "$0") --project PROJECT --topic TOPIC --subscription SUB

Required:
  --project PROJECT         GCP project ID
  --topic TOPIC             Pub/Sub topic name
  --subscription SUB        Pub/Sub subscription name

Optional:
  --help, -h                Show this help
EOF
}

PROJECT="${LOCALGCP_PROJECT:-my-gcp-project}"
TOPIC=""
SUBSCRIPTION=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project)      PROJECT="$2";      shift 2 ;;
        --topic)        TOPIC="$2";        shift 2 ;;
        --subscription) SUBSCRIPTION="$2"; shift 2 ;;
        --help|-h)      usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

[[ -z "$PROJECT" ]]      && { echo "Error: --project is required" >&2; exit 1; }
[[ -z "$TOPIC" ]]        && { echo "Error: --topic is required" >&2; exit 1; }
[[ -z "$SUBSCRIPTION" ]] && { echo "Error: --subscription is required" >&2; exit 1; }

GCL=("python3" "${SCRIPT_DIR}/gcloudlocal.py" "--project" "$PROJECT")

"${GCL[@]}" pubsub topics create "$TOPIC"
"${GCL[@]}" pubsub subscriptions create "$SUBSCRIPTION" --topic "$TOPIC"

echo ""
echo "Done."
echo "  Topic:        projects/$PROJECT/topics/$TOPIC"
echo "  Subscription: projects/$PROJECT/subscriptions/$SUBSCRIPTION"
echo "  Mode:         EMULATOR"
