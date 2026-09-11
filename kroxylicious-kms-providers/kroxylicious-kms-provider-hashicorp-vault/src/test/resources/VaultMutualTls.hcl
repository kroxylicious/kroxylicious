listener "tcp" {
  address       = "0.0.0.0:8202"
  tls_cert_file      = "/vault/config/cert.pem"
  tls_key_file       = "/vault/config/key.pem"
  tls_require_and_verify_client_cert = "true"
  tls_client_ca_file = "/vault/config/client-cert.pem"
}
