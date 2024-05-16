# Project Setup Guide

## Requirements

- **Azure CLI**: Ensure you have the [Azure Command-Line Interface (CLI) installed](https://learn.microsoft.com/cli/azure/install-azure-cli), at least 2.60.0.
- **Bash or Windows Subsystem for Linux [WSL](https://learn.microsoft.com/windows/wsl/install)**: You'll need a Bash-compatible shell environment.
- **Databriks Cli**: [Install databriks cli to manipulate cluster](https://learn.microsoft.com/azure/databricks/dev-tools/cli/tutorial), at least v0.219.0

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
    export RESOURCEGROUP_BASE_NAME=<put a name>
    export RESOURCEGROUP=${RESOURCEGROUP_BASE_NAME}-${LOCATION}
    export USERNAME=<azure user name>
```

### 3. Create a Resource Group

Create an Azure resource group with the specified name and location:

```bash
    az group create -n $RESOURCEGROUP -l $LOCATION
```

### 4. Deploy Resources

Deploy your resources using a Bicep template (e.g., `main.bicep`):

```bash
  az deployment group create -f ./main.bicep -g ${RESOURCEGROUP} -p administratorLoginPassword='changePass123!' username=${USERNAME}
```

The bicep deploys:

- User identity for Azure Data Factory
- Azure Data Lake, the previous identity is a collaborator.
- Azure Databricks Workpace, the previous identity is a collaborator.
- Azure Data Factory. The previous identity is asociated
  - The Azure Data Factory contains a Pipeline with 2 activities
  1. Copy Data from New York Health Data to Azure Data Lake bronze folder
  2. A Databricks Notebook execution
- A SQL Database

Note: So far, the deployment of the link service from Azure Data Factory (ADF) to Databricks is not correct. Although it has been deployed, it is not functioning as expected, and I’m unable to determine how to fix it. A new connection based on Managed Service Identity (MSI) needs to be created and associated with the Databricks activity. I will continue moving forward and revisit this issue later. The fix requires manual intervention

### 5. Upload databricks notebook

There is a notebook on the folder `./notebooks`. It is possible to create it manually using azure portal adding the same content or upload using databriks cli.  

[Azure Databricks personal access token authentication](https://learn.microsoft.com/azure/databricks/dev-tools/cli/authentication#--azure-databricks-personal-access-token-authentication)  
To create a personal access token, do the following:

1. In your Azure Databricks workspace, click your Azure Databricks username in the top bar, and then select Settings from the drop down.
1. Click Developer.
1. Next to Access tokens, click Manage.
1. Click Generate new token.
1. (Optional) Enter a comment that helps you to identify this token in the future, and change the token’s default lifetime of 90 days. To create a token with no lifetime (not recommended), leave the Lifetime (days) box empty (blank).
1. Click Generate.
1. Copy the displayed token to a secure location, and then click Done.

```bash
    #  Upload databricks notebook using databriks cli
    export DATABRICKS_WORKPACE_URL=$(az deployment group show -g ${RESOURCEGROUP} --name main --query properties.outputs.databricksWorkpaceUrl.value --output tsv)
    databricks configure --host $DATABRICKS_WORKPACE_URL 
    # For the prompt Personal Access Token, enter the Azure Databricks personal access token for your workspace

    # Upload the local notebooks to your workpace
    databricks sync ./notebooks/ /Users/${USERNAME}/myLib
```

Then the databricks activity need to be selected the notebook that was already created.

### 6. Execute the Azure Data Factory Pipeline

- Go to Azure Data Factory,
- Launch Azure Data Factory studio
- Go to Author/Pipeline -> IngestNYBabyNames_PL
- Add Trigger-> Trigger Now
- Go to monitor and wait to the pipeline success

The pipeline consumes New York Health data. This example works with baby names https://health.data.ny.gov/Health/Baby-Names-Beginning-2007/jxy9-yhdk/data_preview.
The pipe consumes the api and saves the data on Azure Data Lake, folder **bronze**. The data is a json file with baby names from 2007.

Then the databricks notebook is executed, but so far is a simple notebook. The execution could be check, but it is doing nothing.

### 7. Clean Up

When you're done, delete the resources and the resource group:

```bash
DB_MANAGED_RESOURCEGROUP=$(az deployment group show --resource-group ${RESOURCEGROUP} --name main --query properties.outputs.databriksManagedResourceGroup.value -o tsv)

az group delete -n $RESOURCEGROUP -y
az group delete -n $DB_MANAGED_RESOURCEGROUP -y
```
