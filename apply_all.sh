#!/bin/bash

# 1. (Optional) Set up a dedicated namespace to keep things tidy
kubectl create namespace marketplace --dry-run=client -o yaml | kubectl apply -f -
kubectl config set-context --current --namespace=marketplace

echo "🚀 Starting Marketplace Deployment..."

# 2. Apply the Controller (The Brain)
echo "🧠 Deploying Controller..."
kubectl apply -f k8s/controller.yaml

# Wait a few seconds for the controller service to be ready
sleep 5

# 3. Apply the Storage Layer (The Stateful Tier)
echo "💾 Deploying Storage StatefulSet..."
kubectl apply -f k8s/storage.yaml

# 4. Apply the Service Layer (The Scalable Tier)
echo "🌐 Deploying Scalable Service Nodes..."
kubectl apply -f k8s/service.yaml

echo "------------------------------------------------"
echo "✅ All components applied!"
echo "Run 'kubectl get pods' to check status."
echo "------------------------------------------------"