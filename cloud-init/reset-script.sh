#!/bin/bash

# Example helper, not part of the operator runtime.
# Recreates a few Multipass VMs from dfaas-config.yaml. For every name in NODES
# it deletes and purges the VM of that name without asking, then launches a new
# one. Run it from this directory: it passes --cloud-init dfaas-config.yaml.
# Edit NODES, IMAGE and the CPU, RAM and disk values for your host.
# `multipass list` shows the addresses to put in spec.nodes[].ipAddress.
# See "Requirements" in the README for the SSH and user assumptions the
# operator makes about these VMs.

# Example VM names.
NODES=("dfaas-node-1" "dfaas-node-2" "k6-gen-1")

# Ubuntu image to launch.
IMAGE="26.04"

# VM resources (adjust to your host).
CPUS="2"
RAM="4G"
DISK="20G"

echo "--- Resetting Multipass VMs ---"

for NAME in "${NODES[@]}"; do
    echo "[*] Deleting $NAME..."
    multipass delete "$NAME" --purge 2>/dev/null

    echo "[*] Creating $NAME..."
    multipass launch --name "$NAME" --cloud-init dfaas-config.yaml \
        --cpus "$CPUS" --memory "$RAM" --disk "$DISK" "$IMAGE"

    echo "[V] $NAME created."
done

echo "--- Done ---"
