Here's some information that might be helpful when developing this module.

# Integration tests

The integration tests rely on Fortanix DSM SaaS.   The tests skip unless the following
three environment variables are provided:

* `KROXYLICIOUS_KMS_FORTANIX_API_ENDPOINT` - Endpoint URL of the Fortanix https://api.uk.smartkey.io/
* `KROXYLICIOUS_KMS_FORTANIX_ADMIN_API_KEY` - API key for the Fortanix - this will be used by the test facade. This user needs
   privileges to perform actions like create/delete/rotate keys.
* `KROXYLICIOUS_KMS_FORTANIX_API_KEY` - API key for the Fortanix - this will be used by the production KMS code.

If you don't have a Fortanix DSM SaaS account, create a trial account.   Create 'apps' for admin and the KMS, selecting the
"API Key" as the authentication method.   The "API Key" has two sub-options - a single sting API Key, or a username/password form.
It is the former that we use.   Copy the single string "API Key".  Use those values to set the two API_KEY environment variables.

Once you've done these steps you can run the the io.kroxylicious.kms.service.KmsIT and io.kroxylicious.proxy.encryption.RecordEncryptionFilterIT.
The integration test will execute the Fortanix KMS integration. You can set `KROXYLICIOUS_KMS_FACADE_CLASS_NAME_FILTER` to `.*Fortanix.*` so that
only the integrations for the other KMS providers are skipped.

## CLI

Having the CLI installed is not required, but it is useful when enhancing the Fortanix DSM integration.

```
python3 -m venv ./venv
. ./venv/bin/activate
pip3 install sdkms-cli

export FORTANIX_API_ENDPOINT=https://api.uk.smartkey.io
sdkms-cli user-login  --username xxxx@yyyy.zzz
```

