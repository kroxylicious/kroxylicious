# Multi Tenant Example on Kubernetes.

This example will demonstrate the `MultiTenantTransformationFilter` filter which provides a Multi-Tenant solution for
the Apache Kafka(tm) where one Kafka cluster is present to tenants as if it were many.

The `MultiTenantTransformationFilter` filter works by intercepting all Kafka RPCs that reference resources (such as
topic names and consumer group names). On the request path, the resource name references are prefixed by with a
tenant key. On the response path, the reverse process is applied. Additionally, Kafka RPCs that list resources filter
resources so that only resources that belong to the tenant are returned. In this way, the tenant is given the illusion
of a private cluster.

This filter is a work in progress.
