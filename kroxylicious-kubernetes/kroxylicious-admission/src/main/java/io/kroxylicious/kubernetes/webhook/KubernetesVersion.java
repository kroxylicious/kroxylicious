/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import java.util.HashMap;
import java.util.Map;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Kubernetes cluster version and feature gate information, used to determine
 * which pod injection strategy to use.
 */
record KubernetesVersion(int major, int minor, Map<String, Boolean> featureGates) implements Comparable<KubernetesVersion> {

    static final String SIDECAR_CONTAINERS_GATE = "SidecarContainers";
    static final String IMAGE_VOLUME_GATE = "ImageVolume";

    KubernetesVersion(int major, int minor) {
        this(major, minor, Map.of());
    }

    KubernetesVersion {
        if (major < 1) {
            throw new IllegalArgumentException("Major must be at least 1");
        }
        else if (minor < 0) {
            throw new IllegalArgumentException("Minor must be at least 0");
        }
        featureGates = Map.copyOf(featureGates);
    }

    @Override
    public int compareTo(@NonNull KubernetesVersion o) {
        int cmp = Integer.compare(major, o.major);
        if (cmp == 0) {
            cmp = Integer.compare(minor, o.minor);
        }
        return cmp;
    }

    /**
     * Detects whether the cluster supports native sidecar containers.
     * Enabled by default since Kubernetes 1.29 (beta). On 1.28 (alpha),
     * requires the {@code SidecarContainers} feature gate to be explicitly enabled.
     */
    @VisibleForTesting
    boolean supportedNativeSidecar() {
        Boolean gate = featureGates.get(SIDECAR_CONTAINERS_GATE);
        if (gate != null) {
            return gate;
        }
        return isAtLeast(1, 29);
    }

    /**
     * Detects whether the cluster supports OCI image volumes.
     * Enabled by default since Kubernetes 1.35. On 1.31-1.34 the
     * {@code ImageVolume} feature gate must be explicitly enabled.
     */
    boolean supportsOciImageVolumes() {
        Boolean gate = featureGates.get(IMAGE_VOLUME_GATE);
        if (gate != null) {
            return gate;
        }
        return isAtLeast(1, 35);
    }

    private boolean isAtLeast(int reqMajor, int reqMinor) {
        return this.compareTo(new KubernetesVersion(reqMajor, reqMinor)) >= 0;
    }

    /**
     * Parses a feature gates string of the form {@code "Gate1=true,Gate2=false"}.
     *
     * @return a mutable map of gate name to enabled state
     */
    static Map<String, Boolean> parseFeatureGates(@NonNull String featureGatesStr) {
        Map<String, Boolean> gates = new HashMap<>();
        if (featureGatesStr.isBlank()) {
            return gates;
        }
        for (String entry : featureGatesStr.split(",")) {
            String[] parts = entry.strip().split("=", 2);
            if (parts.length == 2) {
                gates.put(parts[0].strip(), Boolean.parseBoolean(parts[1].strip()));
            }
        }
        return gates;
    }
}
