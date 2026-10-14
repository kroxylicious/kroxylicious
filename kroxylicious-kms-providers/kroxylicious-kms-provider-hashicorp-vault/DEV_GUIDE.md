Here's some information that might be helpful when developing this module.

# Testing Kubernetes Authentication with Minikube

The HashiCorp Vault KMS provider supports Kubernetes ServiceAccount authentication, allowing Kroxylicious to authenticate to Vault using the pod's ServiceAccount token instead of static tokens.

## Automated Setup Script

The `scripts/vault-k8s-auth-minikube.sh` script provides automated setup for testing Vault Kubernetes authentication locally (outside Kubernetes) against Vault running in Minikube:
- Installs Vault in Minikube with Kubernetes auth configured
- Generates Kroxylicious configuration for local testing
- Extracts a ServiceAccount JWT from Kubernetes to use locally
- This mimics what would happen inside a pod (where the JWT is automatically mounted)
- Requires manual port-forwarding to access Vault
- Assumes Kafka is running on localhost:9092
- Provides step-by-step instructions for testing

```bash
./scripts/vault-k8s-auth-minikube.sh setup local
```

The script also:
- Creates necessary namespaces, ServiceAccounts, and RBAC bindings
- Configures Vault Transit engine with appropriate policies
- Creates a test KEK (Key Encryption Key)
- Generates the configuration file in the module's `target/` directory
- Displays comprehensive testing instructions

To remove all resources:
```bash
./scripts/vault-k8s-auth-minikube.sh cleanup
```

The script output includes all necessary commands and verification steps for testing encryption end-to-end.

In-cluster testing is covered by the system test `AlternativeAuthRecordEncryptionST` (see [Testing in a Real Kubernetes Cluster](#testing-in-a-real-kubernetes-cluster)).

## Quick Start for Off Cluster Testing

1. **Run the setup script** to install Vault in Minikube:
   ```bash
   ./scripts/vault-k8s-auth-minikube.sh setup
   ```

2. **Port-forward Vault** (in a separate terminal):
   ```bash
   kubectl port-forward -n vault svc/vault 8200:8200
   ```

3. **Extract a ServiceAccount token**:
   ```bash
   kubectl create token kroxylicious-sa -n kroxylicious --duration=24h > /tmp/sa-token.txt
   ```

4. **Create a Kroxylicious config** using Kubernetes auth (the setup script also generates an equivalent one in the module's `target/` directory):
   ```yaml
   clusterDefinitions:
     - name: demo-cluster
       bootstrapServers: localhost:9092

   virtualClusters:
     - name: demo
       target:
         cluster: demo-cluster
       gateways:
         - name: mygateway
           portIdentifiesNode:
             bootstrapAddress: localhost:19092

   filterDefinitions:
     - name: encrypt
       type: RecordEncryption
       config:
         kms: VaultKmsService
         kmsConfig:
           vaultUrl: http://localhost:8200
           credentials:
             kubernetes:
               vaultRole: kroxylicious-vault-role
               serviceAccountTokenFile: /tmp/sa-token.txt
         selector: TemplateKekSelector
         selectorConfig:
           template: "KEK-$(topicName)"

   defaultFilters:
     - encrypt
   ```

5. **Run Kroxylicious** with debug logging to see the authentication flow:
   ```bash
   KROXYLICIOUS_APP_LOG_LEVEL=DEBUG kroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh \
     --config config.yaml
   ```

6. **Create a test topic and send messages**:
   ```bash
   # Create topic (the filter does not create KEKs; the setup script created KEK-test-topic for this topic)
   kafka-topics.sh --bootstrap-server localhost:19092 --create --topic test-topic --partitions 1 --replication-factor 1

   # Produce encrypted message
   echo "Hello World" | kafka-console-producer.sh --bootstrap-server localhost:19092 --topic test-topic

   # Consume decrypted message
   kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic test-topic --from-beginning

   # Consume directly from Kafka (records are encrypted)
   kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning
   ```

7. **Observe the authentication flow** in the logs. On startup, the Vault KMS logs the login URL it resolved, in a line similar to:
   ```
   INFO  io.kroxylicious.kms.provider.hashicorp.vault.VaultKmsService - Resolved Vault Kubernetes Auth Login URL authUrl=http://localhost:8200/v1/auth/kubernetes/login
   ```
   The login itself happens when the first record needs encrypting and is not logged. To check the JWT to Vault token exchange independently:
   ```bash
   curl -s --request POST \
     --data "{\"role\": \"kroxylicious-vault-role\", \"jwt\": \"$(cat /tmp/sa-token.txt)\"}" \
     http://localhost:8200/v1/auth/kubernetes/login | jq -r '.auth.client_token'
   ```

## Testing in a Real Kubernetes Cluster

In-cluster testing is covered by the system test `AlternativeAuthRecordEncryptionST` (in `kroxylicious-systemtests`). It deploys the proxy via the operator under a dedicated ServiceAccount (`KafkaProxy.spec.infrastructure.serviceAccountName`), binds a Vault role to that ServiceAccount, and verifies records are encrypted at rest and decrypted through the proxy. See "Running system tests locally" in the top-level [DEV_GUIDE.md](../../DEV_GUIDE.md).

## Testing Token Renewal

The Kubernetes token provider refreshes the Vault token before it expires (at 80% of its lease). This is covered by `KubernetesTokenProviderTest`. The provider does not log renewal, so to observe it manually, configure a short TTL for the Vault role and keep producing records past the TTL:
```bash
kubectl exec -n vault vault-0 -- vault write auth/kubernetes/role/kroxylicious-vault-role \
  bound_service_account_names=kroxylicious-sa \
  bound_service_account_namespaces=kroxylicious \
  policies=kroxylicious_encryption_filter_policy \
  ttl=5m
```

## Testing with Static Tokens (for comparison)

To compare Kubernetes auth with static token authentication:

```yaml
filterDefinitions:
  - name: encrypt
    type: RecordEncryption
    config:
      kms: VaultKmsService
      kmsConfig:
        vaultUrl: http://localhost:8200
        credentials:
          vaultToken:
            token:
              password: "hvs.CAESIF..."  # Static token
              # OR
              # passwordFile: "/path/to/token/file"
      selector: TemplateKekSelector
      selectorConfig:
        template: "KEK-$(topicName)"
```

## Troubleshooting

### Authentication Failures

1. **Check ServiceAccount exists**:
   ```bash
   kubectl get sa kroxylicious-sa -n kroxylicious
   ```

2. **Verify ClusterRoleBinding**:
   ```bash
   kubectl get clusterrolebinding vault-auth-delegator -o yaml
   ```

3. **Check Vault role configuration**:
   ```bash
   kubectl exec -n vault vault-0 -- vault read auth/kubernetes/role/kroxylicious-vault-role
   ```

4. **Verify Vault can reach Kubernetes API**:
   ```bash
   kubectl exec -n vault vault-0 -- vault write auth/kubernetes/config \
     kubernetes_host="https://kubernetes.default.svc:443"
   ```

### Token Expiration Issues

If tokens expire unexpectedly:

1. **Check token TTL** in the Vault role:
   ```bash
   kubectl exec -n vault vault-0 -- vault read auth/kubernetes/role/kroxylicious-vault-role
   ```

2. **Check the ServiceAccount token file has not expired.** Tokens created with `kubectl create token` have a fixed lifetime; re-run step 3 of the Quick Start to refresh `/tmp/sa-token.txt`.

3. **Check for clock skew** between Vault and the machine running Kroxylicious

### Network Connectivity

If Kroxylicious can't reach Vault:

1. **Check the port-forward is still running**, and test connectivity from the machine running Kroxylicious:
   ```bash
   curl -v http://localhost:8200/v1/sys/health
   ```

2. **Check the login URL** in the startup log (`Resolved Vault Kubernetes Auth Login URL`) matches your `vaultUrl` and `authPath`

## Security Considerations

### Production Deployment

For production deployments:

1. **Use TLS** for Vault communication:
   ```yaml
   kmsConfig:
     vaultUrl: https://vault.vault.svc.cluster.local:8200
     tls:
       trust:
         storeFile: /path/to/truststore.jks
         storePassword: changeit
         storeType: JKS
   ```

2. **Use proper Vault storage backend** (Consul, Raft, etc.) instead of dev mode

3. **Enable Vault audit logging**:
   ```bash
   kubectl exec -n vault vault-0 -- vault audit enable file file_path=/vault/logs/audit.log
   ```

4. **Implement least-privilege policies** - only grant the minimum permissions needed

5. **Rotate ServiceAccount tokens** regularly

6. **Monitor token renewal** and set up alerts for failures

7. **Use Vault namespaces** for multi-tenancy if needed

### Token Lifecycle

- ServiceAccount tokens are automatically mounted at `/var/run/secrets/kubernetes.io/serviceaccount/token`
- Kubernetes rotates these tokens periodically (default: 1 hour)
- Kroxylicious reads the token file on each authentication attempt, picking up rotations automatically
- Vault tokens obtained via Kubernetes auth have a configurable TTL (default: 24h in examples)
- Kroxylicious renews Vault tokens at 80% of their TTL

## Integration Tests

The module includes integration tests that verify Kubernetes authentication:

```bash
# Run the module's tests, including integration tests
mvn verify -pl kroxylicious-kms-providers/kroxylicious-kms-provider-hashicorp-vault

# Run specific Kubernetes auth test
mvn verify -pl kroxylicious-kms-providers/kroxylicious-kms-provider-hashicorp-vault -Dit.test=VaultKmsKubernetesAuthIT
```

The test uses Testcontainers to:
- Start a Vault container
- Mock the Kubernetes API (OIDC discovery, JWKS, TokenReview)
- Generate a valid ServiceAccount JWT
- Configure Vault Kubernetes auth
- Verify Kroxylicious can authenticate and perform KEK operations

## References

- [HashiCorp Vault Kubernetes Auth](https://developer.hashicorp.com/vault/docs/auth/kubernetes)
- [Vault Transit Engine](https://developer.hashicorp.com/vault/docs/secrets/transit)
- [Kubernetes TokenReview API](https://kubernetes.io/docs/reference/kubernetes-api/authentication-resources/token-review-v1/)
- [Kroxylicious Record Encryption](https://kroxylicious.io/kroxylicious/)
