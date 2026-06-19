#!/bin/bash
# Create 1000 VirtualKafkaCluster resources with dangling references in default namespace
# This slows down VKC informer cache during operator startup

COUNT=${1:-1000}

echo "#Creating $COUNT VirtualKafkaClusters in default namespace..."

for i in $(seq 0 $((COUNT - 1))); do
  cat <<EOF
apiVersion: kroxylicious.io/v1alpha1
kind: VirtualKafkaCluster
metadata:
  name: vkc-bulk-$i
  namespace: default
spec:
  proxyRef:
    name: nonexistent-proxy
  targetKafkaServiceRef:
    name: nonexistent-service
  ingresses:
  - ingressRef: 
      name: foo
---
EOF
done

echo "# Generated $COUNT VirtualKafkaClusters" >&2
