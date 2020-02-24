#!/bin/bash

if [ -z "$1" ]; then
	echo "Missing first argument: path to db folder"
	exit 1
fi

if [ -z "$2" ]; then
	JOB_SLOTS=4
else
	JOB_SLOTS=$2
fi

# try possible locations of hta_check
CHECK_BASE="."
if [ "$(pwd)" == "$(basename "$0")" ]; then
  # we are in <repo root>/helpers
  CHECK_BASE="$(pwd)/.."
fi

if [ -n "$HTA_CHECK_BIN" ] && [ -f "$HTA_CHECK_BIN" ]; then
  # HTA_CHECK_BIN is set and exists, skip search
  pass
elif [ -f "$CHECK_BASE/build/hta_check" ]; then
  HTA_CHECK_BIN="$CHECK_BASE/build/hta_check"
elif [ -f "$CHECK_BASE/hta_check" ]; then
  HTA_CHECK_BIN="$CHECK_BASE/hta_check"
else
  echo "Can't find hta_check. Please set HTA_CHECK_BIN and run again"
  exit
fi

FILES_TO_REPAIR_FILE="$(pwd)/corrupted_metrics_$(date +'%Y-%m-%d-%H%M%S')"
FILES_TO_REPAIR_DIR="$(pwd)/corrupted_metrics_parallels_resultdir_$(date +'%Y-%m-%d-%H%M%S')"
FILES_TO_REPAIR_TMP_FILE="$(pwd)/corrupted_metrics_$(date +'%Y-%m-%d-%H%M%S').tmp"

echo "Corrupted metrics are listed in $FILES_TO_REPAIR_FILE"
echo "$FILES_TO_REPAIR_TMP_FILE contains all detected errors"

ls "$1" | parallel --load 100% --noswap --jobs "$JOB_SLOTS" --results "$FILES_TO_REPAIR_DIR" --eta "$HTA_CHECK_BIN" --fast "$1"/{} > /dev/null

awk '{ print FILENAME, $0}' "$FILES_TO_REPAIR_DIR"/**/**/stderr > "$FILES_TO_REPAIR_TMP_FILE"
cut -f1 -d " " "$FILES_TO_REPAIR_TMP_FILE" | sort -u | cut -f7 -d/ | grep -v -E ".*\.backup-[0-9]+$" > "$FILES_TO_REPAIR_FILE"

rm -r "$FILES_TO_REPAIR_DIR"