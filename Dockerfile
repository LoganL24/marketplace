# 1. Use a slim Python image for a smaller footprint
FROM python:3.11-slim

# 2. Set the working directory
WORKDIR /app

# 3. Install build-essential for gRPC compilation (if needed)
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

# 4. Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy the entire project structure
# This ensures src/, proto/, and all utils are available
COPY . .

# 6. Generate gRPC code
# We use -I. so the imports in the generated files match our folder structure
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/src/marketplace.proto

# 7. Set PYTHONPATH so Python can find 'src' and 'proto'
ENV PYTHONPATH=/app