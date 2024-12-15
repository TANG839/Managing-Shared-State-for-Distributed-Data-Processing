#!/bin/bash
USERNAME_REG="cbdpgroup34"
REGISTRY_NAME="cbdpgroup34"
PASSWORD="gGfY2v0vV6go7T2HYZ+1zD2RSjcfbCxG/iFYRvEO6d+ACRAWvjY+"
# Number of workers to start (can be passed as argument)
workers=${1:-3}  # Default to 3 if no argument provided

# Validate input
if ! [[ "$workers" =~ ^[0-9]+$ ]]; then
    echo "Please provide a valid number of workers"
    exit 1
fi

# Deploy coordinator
echo "Deploying coordinator..."
az container create \
    -g cbdp-resourcegroup \
    --vnet cbdpVnet \
    --subnet cbdpSubnet \
    --restart-policy Never \
    --name coordinator \
    --os-type Linux \
    --cpu 1 \
    --memory 1.5 \
    --image "$REGISTRY_NAME.azurecr.io/cbdp_coordinator" \
    --registry-username $USERNAME_REG \
    --registry-password $PASSWORD

# Get coordinator IP
COORDINATOR=$(az container show \
    --resource-group cbdp-resourcegroup \
    --name coordinator \
    --query ipAddress.ip \
    --output tsv)

echo "Coordinator IP: $COORDINATOR"

# Deploy workers in parallel
echo "Deploying $workers workers..."
for ((i=1; i<=$workers; i++))
do
    worker_name="worker-$(date +%s)-$RANDOM"
    echo "Starting $worker_name..."
    az container create \
        -g cbdp-resourcegroup \
        --vnet cbdpVnet \
        --subnet cbdpSubnet \
        --restart-policy Never \
        --name "$worker_name" \
        --cpu 1 \
        --memory 1.5 \
        --image "$REGISTRY_NAME.azurecr.io/cbdp_worker" \
        --environment-variables CBDP_COORDINATOR="$COORDINATOR" \
        --registry-username $USERNAME_REG \
        --os-type Linux \
        --registry-password $PASSWORD &
done


echo "Deployment completed!"
echo "Use 'az container list -g cbdp-resourcegroup' to check status"