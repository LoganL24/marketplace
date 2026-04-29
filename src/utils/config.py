import os

# --- Service Node Config ---
# The port where the Marketplace API (the frontend) lives
SERVICE_PORT = int(os.getenv("SERVICE_PORT", 50050))

# --- Storage Node Config ---
# Local port for the gRPC server to bind to
NODE_PORT = int(os.getenv("NODE_PORT", 50051))

# --- Controller/Discovery Config ---
CONTROLLER_HOST = os.getenv("CONTROLLER_HOST", "host.docker.internal")
CONTROLLER_PORT = int(os.getenv("CONTROLLER_PORT", 50050))
CONTROLLER_TARGET = f"{CONTROLLER_HOST}:{CONTROLLER_PORT}"

# --- Replication Config (Important for your Service Node) ---
# These default to localhost for your current "Stage 1" testing
STORAGE_PRIMARY_ADDR = os.getenv("STORAGE_PRIMARY_ADDR", "localhost:50051")
STORAGE_BACKUP_ADDR = os.getenv("STORAGE_BACKUP_ADDR", "localhost:50052")

DOCKER_IMAGE = "marketplace-node:latest"
DOCKER_NETWORK = "marketplace-network"