1. Requirements
* azure cli
* It is bash, WLS

1. Azure login 

```bash
az login
# az account set --subscription xxx
```

1. Set Environmet

```bash
export LOCATION=westus
export RESOURCEGROUP_BASE_NAME=far-test
export RESOURCEGROUP=${RESOURCEGROUP_BASE_NAME}-${LOCATION}
```

1. Create resource group  

```bash
az group create -n $RESOURCEGROUP -l $LOCATION
```

1. Create resources

```bash
az deployment group create -f ./main.bicep -g ${RESOURCEGROUP}
```

1. Delete resources

```bash
az group delete -n $RESOURCEGROUP -y
```
