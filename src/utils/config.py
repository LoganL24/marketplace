import os

# --- General Network Config ---
# In K8s, we'll set CONTROLLER_HOST to "controller-service"
CONTROLLER_HOST = os.getenv("CONTROLLER_HOST", "localhost")
CONTROLLER_PORT = os.getenv("CONTROLLER_PORT", "50050")
CONTROLLER_ADDRESS = f"{CONTROLLER_HOST}:{CONTROLLER_PORT}"

# --- Node Specific Config ---
# The port this specific container will listen on
NODE_PORT = os.getenv("NODE_PORT", "50051")

# The IP or Hostname of THIS pod (K8s will provide this via POD_IP env)
# If local, we just use localhost.
MY_POD_NAME = os.getenv("POD_IP", "localhost")
if "storage-" in MY_POD_NAME:
    MY_ADDRESS = f"{MY_POD_NAME}.storage-service"
else:
    MY_ADDRESS = MY_POD_NAME

# --- Service Layer Config ---
# The port the Marketplace (ServiceNode) listens on
SERVICE_PORT = os.getenv("SERVICE_PORT", "50053")