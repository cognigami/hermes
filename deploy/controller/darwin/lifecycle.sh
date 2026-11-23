#!/bin/sh
set -eu
HERMES_ASSETS=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$HERMES_ASSETS/service.sh"
operation=${1-}; test "$#" -gt 0 && shift
hermes_main controller "$operation" "$@"
