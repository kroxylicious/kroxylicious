Here's some information that might be helpful when developing this module.

# Testing the ability to authenticate with EC2 metadata

In order to test this feature, you need to deploy Kroxylicious to an EC2 instance. The 
EC2 instance needs to be associated with a properly configured IAM role. The EC2 credentials
provider will use that to authenticate.

Here's some instructions that will help you test end to end.  For simplicity, these instructions
co-locate a Kafka Broker on the same instance.  Of course, you can choose to run the Kafka
Cluster in any network accessible location.  These instructions also assume you want to run
your Kafka client off-EC2.

1. Create an EC2 instance on AWS - `t2.micro` instance will suffice.
2. Enable SSH with key pair. The remainder of the instructions assume you've download the key into `.ssh/MyEC2KeyPair.pem`.
3. Enable inbound traffic to port 22 (for SSH) and 9192 - 9195 (for Kafka traffic).
4. Follow the 'Authenticating using AWS EC2 metadata' instructions in https://kroxylicious.io/kroxylicious/#assembly-aws-kms-proxy
   to create the policy, IAM role and assign the IAM role to the EC2 host.
5. SSH into the EC2 instance `ssh  -i "~/.ssh/MyEC2KeyPair.pem" ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com` and install packages ready to run docker containers and kroxylicious.
   ```bash
    sudo yum install -y java maven docker
    sudo service docker start
    sudo usermod -a -G docker ec2-user
   ```
6. Logout/login to EC2 again to pick up the new group.
7. Run Kafka on the EC2 instance (inside a docker container)
   ```bash
   docker run -d -p 9092:9092 --name broker apache/kafka:latest
   ```
8. Scp a Kroxylicious dist to the EC2 instance.
   ```bash
   scp  -i "~/.ssh/MyEC2KeyPair.pem" /kroxylicious-app-*-SNAPSHOT-bin.zip ec2-user@ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com:.
   ```
9. Back on the EC2 instance, unzip Kroxylicious
   ```bash
   unzip kroxylicious-app-*-SNAPSHOT-bin.zip
   ```
10. Create a config file for Kroxylicious updating the virtual clusters bootstrap address to the public address of the EC2 instance.
    ```yaml
     virtualClusters:
       demo:
         targetCluster:
           bootstrapServers: localhost:9092
         clusterNetworkAddressConfigProvider:
           type: PortPerBrokerClusterNetworkAddressConfigProvider
           config:
             bootstrapAddress: ec2-xx-xx-xx-xx.compute-1.amazonaws.com:9192
         logNetwork: false
         logFrames: false
     filters:
     - type: RecordEncryption
       config:
         kms: AwsKmsService
         kmsConfig:
           region: us-east-1
           endpointUrl: https://kms.us-east-1.amazonaws.com
           ec2MetadataCredentials:
             iamRole: KroxyliciousInstance
             # credentialLifetimeFactor: 0.001  you can use a low credentialLifetimeFactor to force Kroxylicious to renew the token frequently
         selector: TemplateKekSelector
         selectorConfig:
           template: "KEK_${topicName}"
    ```
11. Run kroxylicious with logs turned up to see the action of the EC2 provider.
    ```bash
    KROXYLICIOUS_APP_LOG_LEVEL=DEBUG ./kroxylicious-app-*-SNAPSHOT/bin/kroxylicious-start.sh -c config.yaml
    ```
12. Create a KEK in AWS KMS (see user docs) and send/receive some messages.
    ```bash
    kaf --brokers ec2-xx-xx-xx-xx.compute-1.amazonaws.com:9192 topic create foo
    echo "Hello World" | kaf --brokers ec2-xx-xx-xx-xx.compute-1.amazonaws.com:9192 produce foo
    kaf --brokers ec2-xx-xx-xx-xx.compute-1.amazonaws.com:9192 consume foo
    ```
13. Notice you'll see the credentials provider reporting that it is renewing the credential:
    ```
    2024-12-06 15:47:29 <Thread-45> DEBUG io.kroxylicious.kms.provider.aws.kms.credentials.Ec2MetadataCredentialsProvider:160 - Scheduling preemptive refresh of AWS credentials in 21139ms
    2024-12-06 15:47:50 <Thread-47> DEBUG io.kroxylicious.kms.provider.aws.kms.credentials.Ec2MetadataCredentialsProvider:210 - Obtained AWS credentials from EC2 metadata using IAM role KroxyliciousInstance, expiry 2024-12-06T21:39:49Z
    2
    ```
14. Don't forget to de-provision everything you've created in AWS once you are done.
