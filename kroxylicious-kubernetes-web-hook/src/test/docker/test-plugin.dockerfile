#
# Copyright Kroxylicious Authors.
#
# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
#

# Test plugin OCI image for integration tests.
# Contains the simple-transform filter JARs at the image root.
# Used to verify OCI image volume plugin mounting in the sidecar.
FROM scratch
COPY target/test-plugin-jars/*.jar /
