#!/bin/bash

# Configurazione nodi
NODES=("nodoA" "nodoB" "nodoC")
IPS=("192.168.252.4" "192.168.252.5" "192.168.252.6")

# Risorse VM (modifica in base alla tua RAM totale)
CPUS="1"
RAM="2G"
DISK="20G"

echo "--- Inizio Reset Nodi Multipass ---"

for i in "${!NODES[@]}"; do
    NAME=${NODES[$i]}
    IP=${IPS[$i]}

    echo "[*] Eliminazione $NAME..."
    multipass delete $NAME --purge 2>/dev/null

    echo "[*] Creazione $NAME con IP $IP..."
    
    # Lancio dell'istanza con configurazione di rete specifica
    # Nota: Assicurati che il bridge sia quello corretto per la tua sottorete
    multipass launch --name $NAME --cloud-init dfaas-config.yaml --cpus $CPUS --memory $RAM --disk $DISK 26.04

    echo "[V] $NAME creato."
done

echo "--- Setup IP Statici (Opzionale) ---"
echo "Nota: Se Multipass non assegna l'IP corretto al boot,"
echo "dovrai lanciare un comando per configurare netplan dentro le VM."

# Pulizia Known Hosts sul tuo Mac per evitare errori SSH
for IP in "${IPS[@]}"; do
    ssh-keygen -R $IP 2>/dev/null
done

echo "--- Fine. Ora puoi rilanciare Ansible ---"