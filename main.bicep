@description('Location of the data factory.')
param location string = resourceGroup().location

@description('The administrator username of the SQL logical server.')
param administratorLogin string = 'myadminname'

@description('The administrator password of the SQL logical server.')
@secure()
param administratorLoginPassword string

// --- Variables
var uniqueName = uniqueString(resourceGroup().id)
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

var httpNYHealhDataLinkedServiceName = 'httpNYHealhData_LS'
var dataLakeStoreLinkedServiceName = 'dataLakeStore_LS'
var dataFactoryDataSetInName = 'babyNamesNY_DS'
var dataFactoryDataSetOutName = 'storeBronzeBabyName_DS'
var pipelineName = 'IngestNYBabyNames_PL'
var managedResourceGroupName = '${resourceGroup().name}-${workspaceName}'
var bronzeContainerName = 'bronze'


// --- Resources

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  identity: {
    type: 'SystemAssigned'
  }
}

resource httpNYHealhDataLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: httpNYHealhDataLinkedServiceName
  properties: {
    type: 'HttpServer'
    typeProperties: {
      authenticationType: 'Anonymous'
      enableServerCertificateValidation: true
      url: 'https://health.data.ny.gov'
    }
  }
}

resource dataLakeStoreLinkedService 'Microsoft.DataFactory/factories/linkedservices@2018-06-01' = {
  parent: dataFactory
  name: dataLakeStoreLinkedServiceName
  properties: {
    type: 'AzureBlobFS'
    typeProperties: {
        url: 'https://${dataLakeStore.name}.dfs.core.windows.net/'
        accountKey: dataLakeStore.listKeys().keys[0].value
    }
  }
}

resource dataFactoryDataSetIn 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: dataFactory
  name: dataFactoryDataSetInName
  properties: {
    linkedServiceName: {
      referenceName: httpNYHealhDataLinkedService.name
      type: 'LinkedServiceReference'
    }
    type: 'HttpFile'
    typeProperties: {
      format: {
        type: 'JsonFormat'
      }
      relativeUrl: '/resource/jxy9-yhdk.json'
    }
  }
}

resource dataFactoryDataSetOut 'Microsoft.DataFactory/factories/datasets@2018-06-01' = {
  parent: dataFactory
  name: dataFactoryDataSetOutName
  properties: {
    linkedServiceName: {
      referenceName: dataLakeStoreLinkedService.name
      type: 'LinkedServiceReference'
    }
    type: 'Json'
    typeProperties: {
      location: {
        type: 'AzureBlobFSLocation'
        fileSystem: 'bronze'
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
        name: 'IngestNYBabyNamesData'
        type: 'Copy'
        typeProperties: {
          source: {
            type: 'HttpSource'
            httpRequestTimeout: '00:01:40'
          }
          sink: {
            type: 'JsonSink'
            storeSettings: {
              type: 'AzureBlobFSWriteSettings'
            }
            formatSettings: {
              type: 'JsonWriteSettings'
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

resource bronzeContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = {
  parent: blobService
  name: bronzeContainerName
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
