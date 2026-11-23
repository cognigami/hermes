#!/bin/sh
set -eu
HERMES_ASSETS=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$HERMES_ASSETS/service.sh"
hermes_main agent install "$@"
