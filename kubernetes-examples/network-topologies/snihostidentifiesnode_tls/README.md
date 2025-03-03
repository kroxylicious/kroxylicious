# SNI Host Identifies Node Network Scheme with Kubernetes LoadBalancer services

This examples illustrates Kroxylicious's SNI Host Identifies Node scheme working in consort with the
LoadBalancer service type of Kubernetes.  The virtual cluster is exposed to the host's network utilising 
a single port.

In the example, the host's /etc/host is modified to allow separate DNS names for bootstrap
and the brokers to resolve to the same IP.  In real-life, this would be replaced by a technique such as
wildcard DNS.

