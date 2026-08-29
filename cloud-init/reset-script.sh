#!/bin/bash

# EXAMPLE / local-dev helper — NOT part of the operator runtime.
# Recreates a few Multipass VMs (on macOS) as the author's local test bed,
# provisioned from dfaas-config.yaml. It is machine-specific: edit NODES/IPS,
# the Ubuntu image tag, and CPU/RAM/disk for your host, and note that
# `multipass launch` here has no --network flag, so the IPS below are only used
# for known_hosts cleanup — assign/read the real VM IPs yourself and put them in
# the Environment CR's spec.nodes[].ipAddress. See "Target VM baseline" in the
# README for the SSH/user assumptions the operator makes about these VMs.

# Configurazione nodi (adatta a IP/nomi della tua rete)
NODES=("nodoA" "nodoB" "nodoC" "nodoD")

# Risorse VM (modifica in base alla tua RAM totale)
CPUS="2"
RAM="4G"
DISK="20G"

echo "--- Inizio Reset Nodi Multipass ---"

for i in "${!NODES[@]}"; do
    NAME=${NODES[$i]}

    echo "[*] Eliminazione $NAME..."
    multipass delete $NAME --purge 2>/dev/null

    echo "[*] Creazione $NAME con IP $IP..."
    
    # Lancio dell'istanza con configurazione di rete specifica
    # Nota: Assicurati che il bridge sia quello corretto per la tua sottorete
    multipass launch --name $NAME --cloud-init dfaas-config.yaml --cpus $CPUS --memory $RAM --disk $DISK 26.04

    echo "[V] $NAME creato."
done

# Pulizia Known Hosts sul tuo Mac per evitare errori SSH
for IP in "${IPS[@]}"; do
    ssh-keygen -R $IP 2>/dev/null
done

echo "--- Fine. Ora puoi rilanciare Ansible ---"