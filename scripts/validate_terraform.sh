#!/bin/bash
# validate_terraform.sh — Run `terraform validate` on all generated Terraform directories.
#
# Usage: bash scripts/validate_terraform.sh <output_dir>
#   output_dir  Root directory to search for .tf files (default: output)
#
# Exits non-zero if any directory fails validation.

set -euo pipefail

OUTPUT_DIR="${1:-output}"

if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Output directory not found: $OUTPUT_DIR"
    exit 1
fi

# Collect unique directories that contain .tf files
TF_DIRS=$(find "$OUTPUT_DIR" -name "*.tf" -exec dirname {} \; 2>/dev/null | sort -u)

if [ -z "$TF_DIRS" ]; then
    echo "No .tf files found under $OUTPUT_DIR — skipping Terraform validation"
    exit 0
fi

PASS=0
FAIL=0
ERRORS=""

while IFS= read -r dir; do
    echo "--- Validating: $dir"
    if terraform -chdir="$dir" init -backend=false -input=false -no-color 2>&1 \
        && terraform -chdir="$dir" validate -no-color; then
        echo "    PASS: $dir"
        PASS=$((PASS + 1))
    else
        echo "    FAIL: $dir" >&2
        ERRORS="$ERRORS\n  - $dir"
        FAIL=$((FAIL + 1))
    fi
done <<< "$TF_DIRS"

echo ""
echo "Terraform validation complete: $PASS passed, $FAIL failed"

if [ "$FAIL" -gt 0 ]; then
    echo -e "Failed directories:$ERRORS" >&2
    exit 1
fi
