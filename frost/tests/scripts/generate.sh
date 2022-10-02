#!/usr/bin/env bash
set -e

# Move to the script dir then back to root so this script can be ran from anywhere
script_dir="${0%/*}"
cd "${script_dir}"
cd ../../.. # repo root

FILEPATH=./frost/tests/fixtures/decompressed.bag
COMPRESSED_FILEPATH=./frost/tests/fixtures/compressed.bag


source ./scripts/setup_py.sh
PYTHON=$(get_python)
setup_venv

$PYTHON ./frost/tests/scripts/gen.py --output "$FILEPATH" --count 100 
$PYTHON ./frost/tests/scripts/gen.py --output "$COMPRESSED_FILEPATH" --count 100 --compression lz4
