# marketplace - project 3

## Running the System


### 1. Create and activate a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate   
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate gRPC code from the proto definition

```bash
chmod +x generate_proto.sh
./generate_proto.sh
```

This compiles `proto/src/marketplace.proto` and writes the generated Python stubs into the same directory. It also applies an import fix required on both macOS and Linux.


## Running Locally

Open **three separate terminals** (all with the virtual environment activated) and start each component in order.

### Terminal 1 — Controller

```bash
python -m src.controller
# Listening on port 50050
```

### Terminal 2 — Storage Node (primary)

```bash
python -m src.storage_node
# Connects to controller on localhost:50050
# Registers, is assigned the PRIMARY role
```

### Terminal 3 — Service Node

```bash
python -m src.service_node
# Connects to controller on localhost:50050
# Listens for client requests on port 50053
```
