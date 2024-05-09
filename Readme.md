# Project Setup Guide

## Requirements
- **Azure CLI**: Ensure you have the Azure Command-Line Interface (CLI) installed.
- **Bash or Windows Subsystem for Linux (WSL)**: You'll need a Bash-compatible shell environment.

## Steps

### 1. Azure Login
First, log in to your Azure account using the following command:

```bash
az login
# Optionally, set the default subscription:
# az account set --subscription <subscription_id>
```

### 2. Set Environment Variables
Define the necessary environment variables for your project:

```bash
export LOCATION=centralus
export RESOURCEGROUP_BASE_NAME=far-test
export RESOURCEGROUP=${RESOURCEGROUP_BASE_NAME}-${LOCATION}
```

### 3. Create a Resource Group
Create an Azure resource group with the specified name and location:

```bash
az group create -n $RESOURCEGROUP -l $LOCATION
```

### 4. Deploy Resources
Deploy your resources using a Bicep template (e.g., `main.bicep`):

```bash
az deployment group create -f ./main.bicep -g ${RESOURCEGROUP} -p administratorLoginPassword="changePass123!"
```

### 5. Clean Up (Optional)
When you're done, delete the resources and the resource group:

```bash
DB_MANAGED_RESOURCEGROUP=$(az deployment group show --resource-group ${RESOURCEGROUP} --name main --query properties.outputs.databriksManagedResourceGroup.value -o tsv)

az group delete -n $RESOURCEGROUP -y
az group delete -n $DB_MANAGED_RESOURCEGROUP -y
```
