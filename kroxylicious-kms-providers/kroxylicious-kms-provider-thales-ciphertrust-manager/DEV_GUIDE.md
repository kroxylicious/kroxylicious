Here's some information that might be helpful when developing this module.

# Running the ITs against a real instance of Thales Cipher Trust Manager

## Install and Setup

By default, the RecordEncryption and KMS ITs use a mock CTM implementation.
You can run against a real instance of Thales CTM using the Community Edition running in AWS 
by following these instructions.

1. Subscribe to ['CipherTrust Manager Community Edition'](https://us-east-1.console.aws.amazon.com/marketplace/subscriptions/ab824523-4292-4840-9d4f-db9442097ce0)
2. Launch an instance - choose One-click launch and customise:
   * `c1.medium` instance type (smallest/cheapest)
   * create a security group with port 443 enabled.
   * create a new keypair or choose an existing one available to you.
3. View the instance
4. Once the instance is available, click through to the Public Address to access the CTM console.  The instance
   will be using a self-signed certificate, so you'll need to bypass the browser warning.
5. You may see a brown error box warning you that services are still starting.  Wait until these clear. This may take 5 mins.
6. Login using CTM's default credentials (admin/admin). Change the password. See https://docs-cybersec.thalesgroup.com/bundle/latest-cdsp-cm/page/get_started/deployment/initial-password/index.html for details.
7. Install the `ksctl` command line.  It is available by following the API -> CLI navigation.
8. Login to the CLI
   ```shell
   ksctl login --url https://<ec2 instance> --user admin --nosslverify
   ```
9. Now we need to default the self-signed certificate with one with a server certificate that matches your instance hostname:
   ```shell
   ./kroxylicious-kms-providers/kroxylicious-kms-provider-thales-ciphertrust-manager/scripts/replace-server-cert.sh <ec2-hostname>
   ```
   or to allow for endpoint changes between instance start/stops, pass additional SAN(s).
   ```shell
   ./kroxylicious-kms-providers/kroxylicious-kms-provider-thales-ciphertrust-manager/scripts/replace-server-cert.sh <ec2-hostname> "*.eu-west-1.compute.amazonaws.com"
   ```
   This script updates the CipherTrust Manager's web interface certificate to use the EC2 hostname,
   allowing secure connections without bypassing SSL verification.
   It restarts the web component within CTM to bring the change into effect. 
10. Extract the CA certificate for TLS trust configuration:
   ```shell
    ksctl interfaces certificate get --name web |\
      jq -r '.certificates | [scan("-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----"; "m")] | last' > /tmp/ctm-ca.pem
   ```
   This extracts the CA certificate (last certificate in the chain) which the client must trust 
   to connect to CipherTrust Manager.

## Running the Integration Tests

The KmsIT and RecordEncryptionIT tests detect environment variables and use them to connect to the real
CipherTrust Manager instance instead of the mock server.

### User authentication (using username and password)

To exercise the path where the KMS (and test facade) authenticate as a user, do the following:

```shell
export KROXYLICIOUS_KMS_FACADE_FACTORY_CLASS_NAME_FILTER=".*Cipher.*"
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_API_ENDPOINT=https://ec2-xx-xx-xx-xx.eu-west-1.compute.amazonaws.com
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_AUTH_MODE=PASSWORD
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_USERNAME=admin
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_PASSWORD=your-password
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_TLS_CA_CERT=/tmp/ctm-ca.pem
```

Then run the integration tests:

```shell
KROXYLICIOUS_KMS_FACADE_FACTORY_CLASS_NAME_FILTER='.*Cipher.*' mvn verify -pl kroxylicious-integration-tests -am -Dfailsafe.failIfNoSpecifiedTests=false -DskipUTs=true -Derrorprone.skip=true -P-qa  -Dit.test=KmsIT 
```

### Client authentication (using certificate)

Client certificate authentication is the recommended approach for service accounts like Kroxylicious, as it eliminates the need to manage passwords and provides stronger authentication.

#### One-time setup: Register a client certificate

Client registration is a one-time admin operation. The registered client can then be used for testing.

Use the provided script to automate the registration process:

```shell
./kroxylicious-kms-providers/kroxylicious-kms-provider-thales-ciphertrust-manager/scripts/register-client-cert.sh /tmp
```

The script will:
1. Create a local CA and self-sign it
2. Generate a CSR and private key for the client
3. Issue and sign the client certificate
4. Register the client management profile with admin group
5. Create a registration token tied to the profile
6. Register the client, linking the token to the client certificate
7. Make the web interface trust the new CA (and restart the web service)
8. Save the credentials and IDs to `/tmp/kroxylicious-*` files

The script outputs the environment variables you'll need - copy them for the next step.

#### Running tests with client certificate authentication

Once you have a registered client, configure the environment variables:

```shell
export KROXYLICIOUS_KMS_FACADE_FACTORY_CLASS_NAME_FILTER=".*Cipher.*"
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_API_ENDPOINT=https://ec2-xx-xx-xx-xx.eu-west-1.compute.amazonaws.com
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_AUTH_MODE=CLIENT_CERT
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_ID=$(cat /tmp/kroxylicious-client-id.txt)
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_CERT=/tmp/kroxylicious-client.crt
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_CLIENT_KEY=/tmp/kroxylicious-client.key
export KROXYLICIOUS_KMS_THALES_CIPHERTRUST_TLS_CA_CERT=/tmp/ctm-ca.pem
```

Then run the integration tests:

```shell
KROXYLICIOUS_KMS_FACADE_FACTORY_CLASS_NAME_FILTER='.*Cipher.*' mvn verify -pl kroxylicious-integration-tests -am -Dfailsafe.failIfNoSpecifiedTests=false -DskipUTs=true -Derrorprone.skip=true -P-qa -Dit.test=KmsIT
```

**Note:** The `AUTH_MODE` defaults to `CLIENT_CERT`, so you can omit that variable if desired.

## Tear down

When you're finished with the CipherTrust Manager instance, clean up the resources to avoid ongoing charges:

1. Unregister the test client certificate (if you registered one):
   ```shell
   ./kroxylicious-kms-providers/kroxylicious-kms-provider-thales-ciphertrust-manager/scripts/unregister-client-cert.sh /tmp
   ```
   This removes the client, registration token, profile, and CA from CipherTrust Manager, removes the CA from the web interface trusted CAs list, and deletes the local credential files.

2. Terminate the EC2 instance:
   - Navigate to the [EC2 Console](https://console.aws.amazon.com/ec2/)
   - Select your CipherTrust Manager instance
   - Click **Instance state** → **Terminate instance**
   - Confirm the termination

3. Delete the security group (if you created a custom one):
   - In the EC2 Console, navigate to **Security Groups**
   - Select the security group you created for the CTM instance
   - Click **Actions** → **Delete security groups**
   - Note: You cannot delete a security group while it's still attached to an instance. Wait for the instance to fully terminate first.

4. Delete the SSH key pair (if you created one specifically for this):
   - In the EC2 Console, navigate to **Key Pairs**
   - Select the key pair
   - Click **Actions** → **Delete**
   - Also delete the local `.pem` file from your `~/.ssh/` directory

5. Unsubscribe from the CipherTrust Manager Community Edition (optional):
   - Navigate to [AWS Marketplace Subscriptions](https://console.aws.amazon.com/marketplace/home#/subscriptions)
   - Find **CipherTrust Manager Community Edition**
   - Click **Manage** → **Cancel subscription**
   - Note: You only need to do this if you don't plan to use CTM again. Unsubscribing won't affect already-terminated instances.
