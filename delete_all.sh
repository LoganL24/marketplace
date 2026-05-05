#!/bin/bash
echo "🗑️ Tearing down marketplace..."
kubectl delete -f k8s/service.yaml
kubectl delete -f k8s/storage.yaml
kubectl delete -f k8s/controller.yaml
echo "✨ Cleaned up!"