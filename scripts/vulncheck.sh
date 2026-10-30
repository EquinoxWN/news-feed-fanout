#!/usr/bin/env bash
# Runs govulncheck and fails on any finding.
set -euo pipefail
out=$(go run golang.org/x/vuln/cmd/govulncheck@v1.8.0 ./... 2>&1) || { echo "$out"; echo "::error::govulncheck reported findings or failed"; exit 1; }
echo "$out"
if ! echo "$out" | grep -qE 'No vulnerabilities found|Your code is affected by 0 vulnerabilities'; then
  echo "::error::govulncheck did not complete"; exit 1
fi
echo "vulncheck: no findings"
