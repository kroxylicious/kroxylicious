# Public API

The kroxylicious project follows Kubernetes best practice for versioning custom resource definitions (CRDs) and consider them part of our public API footprint.

However, we do not offer the same API stability guarantees for the Java classes representing these resources, as they are generated code, and we have minimal influence on how they are generated. 
