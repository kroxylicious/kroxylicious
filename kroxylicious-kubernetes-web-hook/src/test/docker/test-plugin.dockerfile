# Test plugin OCI image for integration tests.
# Contains the simple-transform filter JARs at the image root.
# Used to verify OCI image volume plugin mounting in the sidecar.
FROM scratch
COPY target/test-plugin-jars/*.jar /
