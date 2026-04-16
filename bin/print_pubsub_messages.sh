#!/usr/bin/env bash
# Pull and print messages from a Pub/Sub subscription.
#
# Usage:
#   ./bin/print_pubsub_messages.sh \
#       --project my-project \
#       --subscription my-subscription \
#       [--max-messages 10] [--follow] [--no-auto-ack]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

usage() {
    cat <<EOF
Usage: $(basename "$0") --project PROJECT --subscription SUB [options]

Required:
  --project PROJECT         GCP project ID
  --subscription SUB        Pub/Sub subscription name

Optional:
  --max-messages N          Maximum messages to pull per request (default: 10)
  --follow                  Keep pulling until Ctrl-C
  --no-auto-ack             Do not acknowledge pulled messages
  --help, -h                Show this help
EOF
}

PROJECT="${LOCALGCP_PROJECT:-}"
SUBSCRIPTION=""
MAX_MESSAGES=10
FOLLOW=0
NO_AUTO_ACK=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --project)      PROJECT="$2";      shift 2 ;;
        --subscription) SUBSCRIPTION="$2"; shift 2 ;;
        --max-messages) MAX_MESSAGES="$2"; shift 2 ;;
        --follow)       FOLLOW=1;          shift ;;
        --no-auto-ack)  NO_AUTO_ACK=1;     shift ;;
        --help|-h)      usage; exit 0 ;;
        *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
    esac
done

[[ -z "$PROJECT" ]]      && { echo "Error: --project is required" >&2; exit 1; }
[[ -z "$SUBSCRIPTION" ]] && { echo "Error: --subscription is required" >&2; exit 1; }

GCL=("python3" "${SCRIPT_DIR}/gcloudlocal.py" "--project" "$PROJECT")
ARGS=("pubsub" "subscriptions" "pull" "$SUBSCRIPTION" "--max-messages" "$MAX_MESSAGES")
[[ $FOLLOW      -eq 1 ]] && ARGS+=("--follow")
[[ $NO_AUTO_ACK -eq 1 ]] && ARGS+=("--no-auto-ack")

"${GCL[@]}" "${ARGS[@]}"
