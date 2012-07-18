#!/usr/bin/env bash

# bootstrap
export wdir=$(readlink -f "$(dirname "$0")")
source $wdir/bigquery-env.sh

function bigquery_upload {
  if [ $# -lt 3 ]; then
    echo "Usage: bigquery_upload <logfile> <table> <schema>" >&2
    return 1
  fi
  local logfile="$1"; shift
  local table="$1"; shift
  local schema="$1"; shift

  bq --project_id $PROJECT_ID \
     --dataset_id $DATASET_ID \
     load --encoding ISO-8859-1 \
          --field_delimiter $'\xc3\xbe' \
          --skip_leading_rows 1 $table $logfile $schema
}

bigquery_upload "$@"

# vim: set sw=2 sts=2 : #
