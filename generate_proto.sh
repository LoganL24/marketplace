#!/usr/bin/env bash
set -e

# Get the directory where THIS script is located
ROOT="$(cd "$(dirname "$0")" && pwd)"

# Define paths relative to the script location
PROTO_DIR="$ROOT/proto/src"
OUT_DIR="$ROOT/proto/src"
VENV_PYTHON="$ROOT/venv/bin/python"

echo "Generating gRPC code from marketplace.proto..."

# Run the compiler using the venv python
$VENV_PYTHON -m grpc_tools.protoc \
    -I "$PROTO_DIR" \
    --python_out="$OUT_DIR" \
    --pyi_out="$OUT_DIR" \
    --grpc_python_out="$OUT_DIR" \
    "$PROTO_DIR/marketplace.proto"

# Ensure the directory is a Python package
touch "$OUT_DIR/__init__.py"

# Fix the gRPC import bug (standard for Mac/Linux)
if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i '' 's/import marketplace_pb2/from . import marketplace_pb2/' "$OUT_DIR/marketplace_pb2_grpc.py"
else
    sed -i 's/import marketplace_pb2/from . import marketplace_pb2/' "$OUT_DIR/marketplace_pb2_grpc.py"
fi

if ! grep -q '^import grpc.experimental$' "$OUT_DIR/marketplace_pb2_grpc.py"; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' '/^import grpc$/a\
import grpc.experimental
' "$OUT_DIR/marketplace_pb2_grpc.py"
    else
        sed -i '/^import grpc$/a import grpc.experimental' "$OUT_DIR/marketplace_pb2_grpc.py"
    fi
fi

echo "Success! Files generated in $OUT_DIR"