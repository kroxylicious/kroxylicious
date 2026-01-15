# Development Guide

Here's some information that might be helpful when developing this module.

## Testing the ability to Authenticate with Managed Identity

Azure offers an authentication mechanism called Managed Identity, where applications can obtain an auth token
from the machine they are running on, rather than sending credentials to an oauth server to obtain a token.

**NOTE**: There is a script to execute steps 1-6: [deploy.sh](etc/explore-managed-identity/deploy.sh). 
You can tear down with [teardown.sh](etc/explore-managed-identity/teardown.sh).
Before running these scripts you will need the [azure command line tools](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest), and to `az login` into an account with
broad permissions.

**WARNING: Remember to [Tear Down](#12-tear-it-down) at the end of your exploration!**

### 1. Create the Container
* Establish a new **Resource Group** to hold all subsequent resources.

### 2. Define the Security Policy
* Define a new **Custom RBAC Role** at the subscription level with the following data actions:
    * `Microsoft.KeyVault/vaults/keys/read`
    * `Microsoft.KeyVault/vaults/keys/wrap/action`
    * `Microsoft.KeyVault/vaults/keys/unwrap/action`

### 3. Provision the Key Vault and Key
* Create an **Azure Key Vault** within the resource group, ensuring it is configured to use Role-Based Access Control (RBAC) for its permission model. Note the name of the Vault.
* Create a new **RSA Key** inside the Key Vault named `my-app-wrapping-key`.

### 4. Set Up the Network Infrastructure
* Create a **Virtual Network (VNet)**.
* Create a **Network Security Group (NSG)**.
* Add an inbound security rule to the NSG to **allow TCP traffic on port 22 (SSH)**.
* Create a static **Public IP Address**.
* Create a **Network Interface (NIC)** and associate it with the VNet, NSG, and Public IP Address.

### 5. Deploy the Virtual Machine
* Create a **Linux Virtual Machine** (a cheap B-series instance-type will suffice, though >2G memory will help run kafka/proxy smoothly).
* During its creation, **enable the System-Assigned Managed Identity**.
* Attach the previously created NIC to the VM.
* Configure the VM's administrator account to use SSH public key authentication. The remainder of the instructions will assume your private key is stored in `~/.ssh/azure_vm_key`

### 6. Grant Access
* Create a **Role Assignment** with the following configuration:
    * **Principal**: The VM's **System-Assigned Managed Identity**.
    * **Role**: The **Custom RBAC Role** created in step 2.
    * **Scope**: The specific **Key Vault** resource created in step 3.

### 7. Connect to the Virtual Machine
* Locate the **Public IP Address** assigned to the VM.
* Ensure you have the **private SSH key** that corresponds to the public key used during the VM's creation.
* Using an SSH client, initiate a connection to the VM using:
    * The administrator **username**.
    * The VM's **Public IP Address**.
    * The path to your local **private SSH key** file (`~/.ssh/azure_vm_key`).

### 8. Install Kafka
* Install a java 17 JRE: `sudo apt update && sudo apt install openjdk-17-jre-headless`
* Follow the Kafka quickstart https://kafka.apache.org/quickstart to download and run Kafka on the VM.
* Consider tuning down its network thread count and heap memory. You can set `KAFKA_HEAP_OPTS="-Xmx256m"` and
  in `config/server.properties` tune down `num.network.threads=1`, `num.io.threads=1` and set `log.cleaner.enable=false`.

### 9. Install Kroxylicious
* Build the proxy locally and upload the distribution tarball with `scp -i ~/.ssh/azure_vm_key /path/to/kroxylicious-app-*-SNAPSHOT-bin.tar.gz azureuser@${PUBLIC_IP}:/tmp/kroxy.tar.gz`
* From within the VM, unpack `/tmp/kroxy.tar.gz` into the home directory.
* Edit `~/kroxylicious-${version}/config/example-proxy-config.yaml` and replace the contents with:
    ```yaml
    management:
      endpoints:
        prometheus: {}
    virtualClusters:
      - name: demo
        targetCluster:
          bootstrapServers: localhost:9092
        gateways:
        - name: mygateway
          portIdentifiesNode:
            bootstrapAddress: localhost:9192
        logNetwork: false
        logFrames: false
    filterDefinitions:
    - name: encrypt
      type: RecordEncryption
      config:
        kms: AzureKeyVaultKmsService
        kmsConfig:
          managedIdentityCredentials:
            targetResource: https://vault.azure.net
          keyVaultName: ${my-vault-name}
          keyVaultHost: vault.azure.net
        selector: TemplateKekSelector
        selectorConfig:
          template: "my-app-wrapping-key"
    defaultFilters:
      - encrypt
    ```
  replacing `${my-vault-name}` with the name of your Key Vault from step 3.
* Start the proxy with `JAVA_OPTS="-Xmx256m" ~/kroxylicious-${version}/bin/kroxylicious-start.sh --config config/example-proxy-config.yaml`
### 10. Produce Messages to the Proxy
* From within the VM (start another SSH session), navigate to your Kafka distribution fom Step 8 and run `bin/kafka-console-producer.sh --topic my-secure-topic --bootstrap-server localhost:9192` and produce some records.
* Stop the producer

### 11. Consume Messages and observe they are encrypted
* Run `bin/kafka-console-consumer.sh --from-beginning --topic my-secure-topic --bootstrap-server localhost:9192`. You should see the same values you produced.
* Run `bin/kafka-console-consumer.sh --from-beginning --topic my-secure-topic --bootstrap-server localhost:9092`. You should see the encrypted value that we sent to the underlying Kafka broker.

### 12. Tear it down
* On your machine run `./etc/explore-managed-identity/teardown.sh my-keyvault-vm-rg` to remove the resource group.
* Purge the deleted Key Vault (it is soft deleted)