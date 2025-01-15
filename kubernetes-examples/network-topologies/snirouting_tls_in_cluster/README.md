# SNI Networking Scheme with Kubernetes ClusterIP Services

This examples illustrates Kroxylicious's SNI networking scheme exposed with a kubernetes cluster using
ClusterIP services. Note that we need to use a set of services, one for each broker and our convenience
bootstrap service.

We use [cert-manager](https://cert-manager.io) to configure these services as SANs (Service Alternate Name) in the proxy certificate.
