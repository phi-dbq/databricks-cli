#!/usr/bin/env bash

FWDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
(cd "$FWDIR"
 prospector --profile "$FWDIR/prospector.yaml"
)
