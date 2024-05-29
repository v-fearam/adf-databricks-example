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

### 6. [Create an Azure Key Vault-backed secret scope](https://learn.microsoft.com/azure/databricks/security/secrets/secret-scopes#create-an-azure-key-vault-backed-secret-scope)  

Azure key vault contains the secret which will allow Azure Databricks to connect Azure Data Lake. The notebook will get the secrets from a Databricks secret scope.  

1. Go to https://-databricks-instance-/**#secrets/createScope**. Replace -databricks-instance- with the workspace URL of your Azure Databricks deployment. This URL is case sensitive (scope in createScope must be uppercase).

2. Enter the name of the secret scope. Our notebook expect **dataLakeScope**

3. Set Managed Principal to 'All workpace users'

4. Complete dns name and resource id

```bash
  # Get the values from here
  export DATABRICKS_KEY_VAULT_DNS_NAME=$(az deployment group show -g ${RESOURCEGROUP} --name main --query properties.outputs.databricksKeyVaultUrl.value --output tsv)
  export DATABRICKS_KEY_VAULT_RESOURCE_ID=$(az deployment group show -g ${RESOURCEGROUP} --name main --query properties.outputs.databricksKeyVaultResourceId.value --output tsv)
  echo $DATABRICKS_KEY_VAULT_DNS_NAME
  echo $DATABRICKS_KEY_VAULT_RESOURCE_ID
```

### 7. Create Database Objects

It is time to create the star model in the SQL database, which will be populated by the pipeline. The data analyst thinks and designs the model based on the report requirements.

1. Navigate to the resource group using the Azure Portal.
2. Select the SQL Database
3. Select the Query Editor
4. Enter your username and password. The first time you do this, you’ll need to configure the firewall by following the portal instructions.
5. Copy the code from ./sql/star_model.sql, and paste on the Query Editor
6. Execute
7. Review the table that was created and explore any [store procedures](https://learn.microsoft.com/azure/data-factory/connector-sql-server?tabs=data-factory#invoke-a-stored-procedure-from-a-sql-sink)

### 8. Execute the Azure Data Factory Pipeline

- Go to Azure Data Factory,
- Launch Azure Data Factory studio
- Go to Author/Pipeline -> IngestNYBabyNames_PL
- Add Trigger-> Trigger Now
- Go to monitor and wait to the pipeline success

The pipeline consumes New York Health data. This example works with baby names https://health.data.ny.gov/Health/Baby-Names-Beginning-2007/jxy9-yhdk/data_preview.
The pipe consumes the api and saves the data on Azure Data Lake, folder **bronze**. The data is a json file with baby names from 2007.

Then the databricks notebook is executed, but so far is a simple notebook. The execution could be check, but it is doing nothing.

### 9. Execute in database

Navigate to the resource group using the SQL Database-Query Editor again, and execute the following query to retrieve the most common male names used in New York in 2021

```sql

SELECT top 10  n.first_name, SUM(f.count) AS total_count
FROM fact_babynames f
JOIN dim_names n ON f.nameSid = n.sid
JOIN dim_years y ON f.yearSid = y.sid
WHERE n.sex = 'M' AND y.year = 2021
GROUP BY n.first_name
ORDER BY total_count DESC

```

### 10. Clean Up

When you're done, delete the resources and the resource group:

```bash
export DATABRICKS_KEY_VAULT_NAME=$(az deployment group show -g ${RESOURCEGROUP} --name main --query properties.outputs.databricksKeyVaultName.value --output tsv)
export ADF_KEY_VAULT_NAME=$(az deployment group show -g ${RESOURCEGROUP} --name main --query properties.outputs.adfKeyVaultName.value --output tsv)

az group delete -n $RESOURCEGROUP -y
az keyvault purge --name $DATABRICKS_KEY_VAULT_NAME
az keyvault purge --name $ADF_KEY_VAULT_NAME
```
