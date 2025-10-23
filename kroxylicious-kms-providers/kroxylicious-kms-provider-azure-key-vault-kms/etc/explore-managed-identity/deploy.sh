#!/bin/bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )"

RESOURCE_GROUP="my-keyvault-vm-rg"
LOCATION="eastus"
VM_ADMIN_USERNAME="azureuser"
SSH_KEY_NAME="azure_vm_key"

echo "Creating resource group '$RESOURCE_GROUP' in '$LOCATION'..."
az group create --name $RESOURCE_GROUP --location $LOCATION -o none

if [ ! -f ~/.ssh/$SSH_KEY_NAME ]; then
  echo "Generating SSH key pair '~/.ssh/$SSH_KEY_NAME'..."
  ssh-keygen -t rsa -b 2048 -f ~/.ssh/$SSH_KEY_NAME -N ""
  echo "SSH key pair generated."
else
  echo "Using existing SSH key pair '~/.ssh/$SSH_KEY_NAME'."
fi

SSH_PUBLIC_KEY=$(cat ~/.ssh/$SSH_KEY_NAME.pub)

echo "Deploying Bicep template (this may take a few minutes)..."
DEPLOYMENT_OUTPUTS=$(az deployment group create \
  --resource-group $RESOURCE_GROUP \
  --template-file main.bicep \
  --parameters adminUsername=$VM_ADMIN_USERNAME \
               adminSshKeyPublicKey="$SSH_PUBLIC_KEY" \
  --query "properties.outputs" -o json)

if [ -z "$DEPLOYMENT_OUTPUTS" ]; then
  echo "Error: Deployment failed or produced no output."
  exit 1
fi

echo "\n✨ Deployment complete."

VM_IP=$(echo $DEPLOYMENT_OUTPUTS | jq -r '.vmPublicIpAddress.value')
VM_USER=$(echo $DEPLOYMENT_OUTPUTS | jq -r '.vmAdminUsername.value')

echo "VM Created. To connect, use the following command:"
echo "ssh ${VM_USER}@${VM_IP} -i ~/.ssh/${SSH_KEY_NAME}"
