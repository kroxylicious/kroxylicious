/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.webhook;

import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

record KubernetesVersion(int major, int minor) implements Comparable<KubernetesVersion> {
    KubernetesVersion {
        if (major < 1) {
            throw new IllegalArgumentException("Major must be at least 1");
        }
        else if (minor < 0) {
            throw new IllegalArgumentException("Minor must be at least 0");
        }
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
     * Detects whether the cluster supports native sidecar containers (Kubernetes 1.28+).
     */
    @VisibleForTesting
    boolean supportedNativeSidecar() {
        return this.compareTo(new KubernetesVersion(1, 28)) >= 0;
    }

    /**
     * Detects whether the cluster supports OCI image volumes (Kubernetes 1.31+).
     * Note that the {@code ImageVolume} feature gate must also be enabled on the cluster.
     */
    boolean supportsOciImageVolumes() {
        return this.compareTo(new KubernetesVersion(1, 31)) >= 0;
    }
}
