#!/usr/bin/env bash
#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Vault Kubernetes Auth Setup Script for Minikube
# This script sets up HashiCorp Vault with Kubernetes authentication for testing Kroxylicious

set -euo pipefail

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
VAULT_NAMESPACE="${VAULT_NAMESPACE:-vault}"
VAULT_RELEASE="${VAULT_RELEASE:-vault}"
KROXYLICIOUS_NAMESPACE="${KROXYLICIOUS_NAMESPACE:-kroxylicious}"
KROXYLICIOUS_SA="${KROXYLICIOUS_SA:-kroxylicious-sa}"
VAULT_ROLE="${VAULT_ROLE:-kroxylicious-vault-role}"
POLICY_NAME="${POLICY_NAME:-kroxylicious_encryption_filter_policy}"
TEST_KEK="${TEST_KEK:-KEK-test-topic}"

# Logging functions
log_info() {
    printf "${BLUE}[INFO]${NC} %s\n" "$1"
}

log_success() {
    printf "${GREEN}[SUCCESS]${NC} %s\n" "$1"
}

log_warn() {
    printf "${YELLOW}[WARN]${NC} %s\n" "$1"
}

log_error() {
    printf "${RED}[ERROR]${NC} %s\n" "$1"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    local missing_tools=()

    for tool in kubectl helm minikube; do
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done

    if [ ${#missing_tools[@]} -gt 0 ]; then
        log_error "Missing required tools: ${missing_tools[*]}"
        log_error "Please install the missing tools and try again"
        exit 1
    fi

    # Check if minikube is running
    if ! minikube status &> /dev/null; then
        log_error "Minikube is not running. Please start it with: minikube start"
        exit 1
    fi

    log_success "All prerequisites met"
}

# Create namespaces
create_namespaces() {
    log_info "Creating namespaces..."
    kubectl create namespace "$VAULT_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace "$KROXYLICIOUS_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
    log_success "Namespaces created"
}

# Install Vault via Helm
install_vault() {
    log_info "Installing Vault via Helm..."

    # Add HashiCorp Helm repository (suppress output)
    NO_COLOR=1 helm repo add hashicorp https://helm.releases.hashicorp.com >/dev/null 2>&1 || true
    NO_COLOR=1 helm repo update >/dev/null 2>&1

    # Install Vault in dev mode for testing
    # Use NO_COLOR to prevent helm from outputting ANSI color codes
    local helm_output
    if helm_output=$(NO_COLOR=1 helm upgrade --install "$VAULT_RELEASE" hashicorp/vault \
        --namespace "$VAULT_NAMESPACE" \
        --set "server.dev.enabled=true" \
        --set "server.dev.devRootToken=root" \
        --set "injector.enabled=false" \
        --set "ui.enabled=true" \
        --set "server.service.type=ClusterIP" \
        --wait \
        --timeout 5m 2>&1); then
        log_success "Vault installed successfully"
    else
        log_error "Failed to install Vault"
        echo "$helm_output"
        exit 1
    fi
}

# Wait for Vault to be ready
wait_for_vault() {
    log_info "Waiting for Vault pod to be ready..."
    kubectl wait --for=condition=ready pod/vault-0 -n "$VAULT_NAMESPACE" --timeout=300s
    log_success "Vault is ready"
}

# Configure Transit Engine
configure_transit_engine() {
    log_info "Configuring Vault Transit Engine..."

    # Enable transit engine (ignore error if already enabled)
    kubectl exec -n "$VAULT_NAMESPACE" vault-0 -- vault secrets enable transit 2>/dev/null || true

    log_success "Transit engine enabled"
}

# Create Vault policy for Kroxylicious
create_vault_policy() {
    log_info "Creating Vault policy for Kroxylicious..."

    # Create policy that allows Kroxylicious to use transit engine
    # Policy matches documentation: kroxylicious-docs/docs/_modules/record-encryption/hashicorp-vault/con-vault-setup.adoc
    kubectl exec -n "$VAULT_NAMESPACE" vault-0 -- sh -c "cat > /tmp/kroxylicious-policy.hcl <<EOF
path \"transit/keys/KEK-*\" {
  capabilities = [\"read\"]
}
path \"transit/datakey/plaintext/KEK-*\" {
  capabilities = [\"update\"]
}
path \"transit/decrypt/KEK-*\" {
  capabilities = [\"update\"]
}
EOF"

    kubectl exec -n "$VAULT_NAMESPACE" vault-0 -- vault policy write "$POLICY_NAME" /tmp/kroxylicious-policy.hcl

    log_success "Vault policy created"
}

# Create Kroxylicious ServiceAccount
create_kroxylicious_serviceaccount() {
    log_info "Creating Kroxylicious ServiceAccount..."

    kubectl create serviceaccount "$KROXYLICIOUS_SA" -n "$KROXYLICIOUS_NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

    log_success "ServiceAccount created"
}

# Create ClusterRoleBinding for Vault auth-delegator
create_vault_auth_delegator_binding() {
    log_info "Creating ClusterRoleBinding for Vault auth-delegator..."

    # This allows Vault to verify ServiceAccount tokens with the Kubernetes API
    kubectl create clusterrolebinding vault-auth-delegator \
        --clusterrole=system:auth-delegator \
        --serviceaccount="$VAULT_NAMESPACE:vault" \
        --dry-run=client -o yaml | kubectl apply -f -

    log_success "ClusterRoleBinding created"
}

# Configure Kubernetes authentication in Vault
configure_kubernetes_auth() {
    log_info "Configuring Kubernetes authentication in Vault..."

    # Enable Kubernetes auth method (ignore error if already enabled)
    kubectl exec -n "$VAULT_NAMESPACE" vault-0 -- vault auth enable kubernetes 2>/dev/null || true

    # Configure Kubernetes auth to use in-cluster Kubernetes API
    # Vault will use its own ServiceAccount token to authenticate to the K8s API
    kubectl exec -n "$VAULT_NAMESPACE" vault-0 -- vault write auth/kubernetes/config \
        kubernetes_host="https://kubernetes.default.svc"

    log_success "Kubernetes auth configured"
}

# Create Vault role for Kroxylicious
create_vault_role() {
    log_info "Creating Vault role for Kroxylicious..."

    # Create role that binds the policy to the ServiceAccount
    kubectl exec -n "$VAULT_NAMESPACE" vault-0 -- vault write "auth/kubernetes/role/$VAULT_ROLE" \
        bound_service_account_names="$KROXYLICIOUS_SA" \
        bound_service_account_namespaces="$KROXYLICIOUS_NAMESPACE" \
        policies="$POLICY_NAME" \
        ttl=24h 2>&1 | grep -v "^WARNING" || true

    log_success "Vault role created"
}

# Create test KEK
create_test_kek() {
    log_info "Creating test KEK ($TEST_KEK)..."

    # Create a test encryption key
    kubectl exec -n "$VAULT_NAMESPACE" vault-0 -- vault write -f "transit/keys/$TEST_KEK"

    log_success "Test KEK created"
}

# Get Vault service information
get_vault_service_info() {
    log_info "Retrieving Vault service information..."

    # Get the Vault service cluster IP
    VAULT_SERVICE_IP=$(kubectl get svc vault -n "$VAULT_NAMESPACE" -o jsonpath='{.spec.clusterIP}')
    VAULT_SERVICE_URL="http://vault.$VAULT_NAMESPACE.svc.cluster.local:8200"

    log_success "Vault service: $VAULT_SERVICE_URL"
}

# Generate Kroxylicious configuration file
generate_kroxylicious_config() {
    log_info "Generating Kroxylicious configuration file..."

    local config_file="kroxylicious-kms-providers/kroxylicious-kms-provider-hashicorp-vault/target/kroxylicious-vault-k8s-auth-config.yaml"

    # Ensure target directory exists
    mkdir -p "$(dirname "$config_file")"

    cat > "$config_file" <<EOF
#
# Kroxylicious Configuration for Vault Kubernetes Auth
# Generated by: $0 setup
#
# Configuration for LOCAL testing (with port-forward)
#

---
management:
  endpoints:
    prometheus: {}

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
          bootstrapAddress: localhost:9192
    logNetwork: false
    logFrames: false

filterDefinitions:
  - name: recordEncryption
    type: RecordEncryption
    config:
      kms: VaultKmsService
      kmsConfig:
        vaultUrl: http://localhost:8200
        credentials:
          kubernetes:
            vaultRole: $VAULT_ROLE
            serviceAccountTokenFile: /tmp/sa-token.txt
            # Optional: Override default auth mount path
            # authPath: kubernetes
      selector: TemplateKekSelector
      selectorConfig:
        template: "KEK-\$(topicName)"

defaultFilters:
  - recordEncryption
EOF

    log_success "Configuration file saved to: $config_file"
}

# Print testing instructions
print_testing_instructions() {
    local config_file="${1:-kroxylicious-kms-providers/kroxylicious-kms-provider-hashicorp-vault/target/kroxylicious-vault-k8s-auth-config.yaml}"

    # Reset terminal to ensure clean state
    printf '\033[0m'
    tput sgr0 2>/dev/null || true

    printf '\n'
    printf '\033[0;32m╔════════════════════════════════════════════════════════════════════════════╗\033[0m\n'
    printf '\033[0;32m║                    Vault Setup Complete!                                   ║\033[0m\n'
    printf '\033[0;32m╚════════════════════════════════════════════════════════════════════════════╝\033[0m\n'
    printf '\n'

    printf '\033[0;34mVault Information:\033[0m\n'
    printf '  Namespace:        %s\n' "$VAULT_NAMESPACE"
    printf '  Service:          vault.%s.svc.cluster.local:8200\n' "$VAULT_NAMESPACE"
    printf '  Root Token:       root (dev mode)\n'
    printf '  Transit Engine:   Enabled at /v1/transit\n'
    printf '  Test KEK:         %s\n' "$TEST_KEK"
    printf '\n'

    printf '\033[0;34mKubernetes Auth Configuration:\033[0m\n'
    printf '  Vault Role:       %s\n' "$VAULT_ROLE"
    printf '  ServiceAccount:   %s (namespace: %s)\n' "$KROXYLICIOUS_SA" "$KROXYLICIOUS_NAMESPACE"
    printf '  Policy:           %s\n' "$POLICY_NAME"
    printf '\n'

    printf '\033[0;34mTesting Locally (Outside Kubernetes):\033[0m\n'
    printf '\n'
    printf '\033[0;33mPrerequisite:\033[0m A Kafka cluster must be running on localhost:9092 (PLAIN listener)\n'
    printf '\n'
    printf '1. Port-forward Vault service (run in background):\n'
    printf '   \033[1;33mkubectl port-forward -n %s svc/vault 8200:8200 &\033[0m\n' "$VAULT_NAMESPACE"
    printf '\n'
    printf '2. Extract a ServiceAccount token:\n'
    printf '   \033[1;33mkubectl create token %s -n %s --duration=24h > /tmp/sa-token.txt\033[0m\n' "$KROXYLICIOUS_SA" "$KROXYLICIOUS_NAMESPACE"
    printf '\n'
    printf '3. Run Kroxylicious with the generated config (in background):\n'
    printf '   \033[1;33mkroxylicious-app/target/kroxylicious-app-*-bin/kroxylicious-app-*/bin/kroxylicious-start.sh \\\n'
    printf '     --config %s &\033[0m\n' "$config_file"
    printf '\n'
    printf '4. Test encryption by producing and consuming records:\n'
    printf '   \033[1;33m# Produce records through the proxy:\n'
    printf '   kafka-console-producer.sh --bootstrap-server localhost:9192 --topic test-topic\n'
    printf '   > Hello encrypted world\n'
    printf '   > This message is encrypted by Kroxylicious\n'
    printf '   > ^D (Ctrl+D to exit)\n'
    printf '\n'
    printf '   # Consume through proxy (records are decrypted):\n'
    printf '   kafka-console-consumer.sh --bootstrap-server localhost:9192 --topic test-topic --from-beginning\n'
    printf '   # You should see: "Hello encrypted world" and "This message is encrypted by Kroxylicious"\n'
    printf '   # ^C (Ctrl+C to exit)\n'
    printf '\n'
    printf '   # Consume directly from Kafka (records are encrypted - proves encryption works):\n'
    printf '   kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test-topic --from-beginning\n'
    printf '   # You should see encrypted binary data (not readable text)\n'
    printf '   # This proves the records are encrypted at rest in Kafka\n'
    printf '   # ^C (Ctrl+C to exit)\033[0m\n'
    printf '\n'

    printf '\033[0;34mVerify Setup:\033[0m\n'
    printf '\n'
    printf '1. Check Vault status:\n'
    printf '   \033[1;33mkubectl exec -n %s vault-0 -- vault status\033[0m\n' "$VAULT_NAMESPACE"
    printf '\n'
    printf '2. List Kubernetes auth roles:\n'
    printf '   \033[1;33mkubectl exec -n %s vault-0 -- vault list auth/kubernetes/role\033[0m\n' "$VAULT_NAMESPACE"
    printf '\n'
    printf '3. Read the Kroxylicious role:\n'
    printf '   \033[1;33mkubectl exec -n %s vault-0 -- vault read auth/kubernetes/role/%s\033[0m\n' "$VAULT_NAMESPACE" "$VAULT_ROLE"
    printf '\n'
    printf '4. List Transit keys:\n'
    printf '   \033[1;33mkubectl exec -n %s vault-0 -- vault list transit/keys\033[0m\n' "$VAULT_NAMESPACE"
    printf '\n'

    printf '\033[0;34mVerify Kubernetes Auth (Local):\033[0m\n'
    printf '\n'
    printf '1. Test JWT to Vault token exchange:\n'
    printf '   \033[1;33mVAULT_TOKEN=$(curl -s --request POST \\\n'
    printf '     --data '"'"'{"role": "%s", "jwt": "'"'"'$(cat /tmp/sa-token.txt)'"'"'"}'"'"' \\\n' "$VAULT_ROLE"
    printf '     http://localhost:8200/v1/auth/kubernetes/login | \\\n'
    printf '     jq -r '"'"'.auth.client_token'"'"')\n'
    printf '   \n'
    printf '   echo "Vault Token: $VAULT_TOKEN"\033[0m\n'
    printf '\n'
    printf '2. Test transit encryption with the token:\n'
    printf '   \033[1;33mexport VAULT_ADDR=http://localhost:8200\n'
    printf '   export VAULT_TOKEN  # Use token from step 1\n'
    printf '   \n'
    printf '   # List transit keys\n'
    printf '   vault list transit/keys\033[0m\n'
    printf '\n'

    printf '\033[0;34mCleanup:\033[0m\n'
    printf '   \033[1;33m%s cleanup\033[0m\n' "$0"
    printf '   \033[0;33m# Note: Stop any background port-forward processes:\033[0m\n'
    printf '   \033[1;33m# pkill -f "kubectl port-forward.*vault"\033[0m\n'
    printf '\n'

    printf '\033[0;34mConfiguration File:\033[0m\n'
    printf '   \033[1;33m%s\033[0m\n' "$config_file"
    printf '\n'
}

# Setup function
setup() {
    local config_file="kroxylicious-kms-providers/kroxylicious-kms-provider-hashicorp-vault/target/kroxylicious-vault-k8s-auth-config.yaml"

    log_info "Starting Vault Kubernetes Auth setup for Minikube..."
    echo

    check_prerequisites
    create_namespaces
    install_vault
    wait_for_vault
    configure_transit_engine
    create_vault_policy
    create_kroxylicious_serviceaccount
    create_vault_auth_delegator_binding
    configure_kubernetes_auth
    create_vault_role
    create_test_kek
    get_vault_service_info
    generate_kroxylicious_config

    echo
    print_testing_instructions "$config_file"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up Vault Kubernetes Auth setup..."
    echo

    # Delete Helm release (use NO_COLOR to prevent ANSI codes)
    log_info "Uninstalling Vault Helm release..."
    NO_COLOR=1 helm uninstall "$VAULT_RELEASE" -n "$VAULT_NAMESPACE" 2>/dev/null || log_warn "Vault release not found or already deleted"

    # Delete ServiceAccount
    log_info "Deleting Kroxylicious ServiceAccount..."
    kubectl delete serviceaccount "$KROXYLICIOUS_SA" -n "$KROXYLICIOUS_NAMESPACE" 2>/dev/null || log_warn "ServiceAccount not found or already deleted"

    # Delete ClusterRoleBinding
    log_info "Deleting ClusterRoleBinding..."
    kubectl delete clusterrolebinding vault-auth-delegator 2>/dev/null || log_warn "ClusterRoleBinding not found or already deleted"

    # Optionally delete namespaces (commented out by default to preserve other resources)
    # log_info "Deleting namespaces..."
    # kubectl delete namespace "$VAULT_NAMESPACE" 2>/dev/null || log_warn "Vault namespace not found or already deleted"
    # kubectl delete namespace "$KROXYLICIOUS_NAMESPACE" 2>/dev/null || log_warn "Kroxylicious namespace not found or already deleted"

    # Delete generated files
    log_info "Deleting generated files..."
    rm -f kroxylicious-kms-providers/kroxylicious-kms-provider-hashicorp-vault/target/kroxylicious-vault-k8s-auth-config.yaml

    echo
    log_success "Cleanup complete"
}

usage() {
    echo "Usage: $0 {setup|cleanup}"
    echo
    echo "Commands:"
    echo "  setup     - Install and configure Vault with Kubernetes auth for local testing (requires local Kafka)"
    echo "  cleanup   - Remove Vault and related resources"
}

# Main script logic
if [[ $# -ne 1 ]]; then
    usage
    exit 1
fi

case "$1" in
    setup)
        setup
        ;;
    cleanup)
        cleanup
        ;;
    *)
        usage
        exit 1
        ;;
esac
