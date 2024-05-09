@description('Location of the data factory.')
param location string = resourceGroup().location

@description('The administrator username of the SQL logical server.')
param administratorLogin string = 'myadminname'

@description('The administrator password of the SQL logical server.')
@secure()
param administratorLoginPassword string

// --- Variables
var uniqueName = uniqueString(resourceGroup().id)
@description('Name of the Azure storage account that contains the input/output data.')
var storageAccountName ='storage${uniqueName}'
@description('Name of the blob container in the Azure Storage account.')
var blobContainerName = 'blob-${uniqueName}'
@description('Data Factory Name')
var dataFactoryName = 'datafactory-${uniqueName}'
@description('The name of the Azure Databricks workspace to create.')
var workspaceName = 'databricks-workpace-${uniqueName}'
@description('The name of the Azure Data Lake Store to create.')
var datalakeStoreName = 'datalake${uniqueName}'
@description('The name of the SQL logical server.')
var serverName  = 'sqlserver-${uniqueName}'
@description('The name of the SQL Database.')
var  sqlDBName  = 'SampleDB'

var dataFactoryLinkedServiceName = 'LS_ArmtemplateStorage'
var dataFactoryDataSetInName = 'DS_ArmtemplateTestIn'
var dataFactoryDataSetOutName = 'DS_ArmtemplateTestOut'
var pipelineName = 'PL_ArmtemplateSampleCopy'
var managedResourceGroupName = '${resourceGroup().name}-${workspaceName}'


// --- Resources
resource storageAccount 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'

  properties: {
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
  }

  resource defaultBlobService 'blobServices' = {
    name: 'default'
  }
}

resource blobContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: storageAccount::defaultBlobService
  name: blobContainerName
}

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
}

resource dataFactoryLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: dataFactoryLinkedServiceName
  properties: {
    type: 'AzureBlobStorage'
    typeProperties: {
      connectionString: 'DefaultEndpointsProtocol=https;AccountName=${storageAccount.name};AccountKey=${storageAccount.listKeys().keys[0].value}'
    }
  }
}

resource dataFactoryDataSetIn 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: dataFactory
  name: dataFactoryDataSetInName
  properties: {
    linkedServiceName: {
      referenceName: dataFactoryLinkedService.name
      type: 'LinkedServiceReference'
    }
    type: 'Binary'
    typeProperties: {
      location: {
        type: 'AzureBlobStorageLocation'
        container: blobContainerName
        folderPath: 'input'
        fileName: 'emp.txt'
      }
    }
  }
}

resource dataFactoryDataSetOut 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: dataFactory
  name: dataFactoryDataSetOutName
  properties: {
    linkedServiceName: {
      referenceName: dataFactoryLinkedService.name
      type: 'LinkedServiceReference'
    }
    type: 'Binary'
    typeProperties: {
      location: {
        type: 'AzureBlobStorageLocation'
        container: blobContainerName
        folderPath: 'output'
      }
    }
  }
}

resource dataFactoryPipeline 'Microsoft.DataFactory/factories/pipelines@2018-06-01' = {
  parent: dataFactory
  name: pipelineName
  properties: {
    activities: [
      {
        name: 'MyCopyActivity'
        type: 'Copy'
        typeProperties: {
          source: {
            type: 'BinarySource'
            storeSettings: {
              type: 'AzureBlobStorageReadSettings'
              recursive: true
            }
          }
          sink: {
            type: 'BinarySink'
            storeSettings: {
              type: 'AzureBlobStorageWriteSettings'
            }
          }
          enableStaging: false
        }
        inputs: [
          {
            referenceName: dataFactoryDataSetIn.name
            type: 'DatasetReference'
          }
        ]
        outputs: [
          {
            referenceName: dataFactoryDataSetOut.name
            type: 'DatasetReference'
          }
        ]
      }
    ]
  }
}

resource managedResourceGroup 'Microsoft.Resources/resourceGroups@2021-04-01' existing = {
  scope: subscription()
  name: managedResourceGroupName
}

resource databricksWorkpace 'Microsoft.Databricks/workspaces@2018-04-01' = {
  name: workspaceName
  location: location
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId:   managedResourceGroup.id
    parameters: {
      enableNoPublicIp: {
        value: false
      }
    }
  }
}

resource dataLakeStore 'Microsoft.Storage/storageAccounts@2023-04-01' = {
  name: datalakeStoreName
  location: 'eastus'
  sku: {
      name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
      publicNetworkAccess: 'Enabled'
      allowBlobPublicAccess: false
      allowSharedKeyAccess: true
      largeFileSharesState: 'Enabled'
      isHnsEnabled: true
      networkAcls: {
          bypass: 'AzureServices'
          virtualNetworkRules: []
          ipRules: []
          defaultAction: 'Allow'
      }
      supportsHttpsTrafficOnly: true
      accessTier: 'Hot'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-04-01' = {
  name: 'default'
  parent: dataLakeStore
  dependsOn: [
      storageAccount
  ]
  properties: {
      containerDeleteRetentionPolicy: {
          enabled: true
          days: 7
      }
      cors: {
          corsRules: []
      }
      deleteRetentionPolicy: {
          allowPermanentDelete: false
          enabled: true
          days: 7
      }
  }
}

resource fileService 'Microsoft.Storage/storageAccounts/fileServices@2023-04-01' = {
  name: 'default'
  parent: dataLakeStore
  dependsOn: [
      storageAccount
  ]
  properties: {
      protocolSettings: {
          smb: {}
      }
      cors: {
          corsRules: []
      }
      shareDeleteRetentionPolicy: {
          enabled: true
          days: 7
      }
  }
}

resource sqlServer 'Microsoft.Sql/servers@2022-05-01-preview' = {
  name: serverName
  location: location
  properties: {
    administratorLogin: administratorLogin
    administratorLoginPassword: administratorLoginPassword
  }
}

resource sqlDB 'Microsoft.Sql/servers/databases@2022-05-01-preview' = {
  parent: sqlServer
  name: sqlDBName
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
  }
}

output name string = dataFactoryPipeline.name
output resourceId string = dataFactoryPipeline.id
output databriksManagedResourceGroup string = managedResourceGroupName
output location string = location
