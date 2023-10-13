/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.cmd;

import java.io.File;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Abstraction for a kubernetes client.
 * @param <K>  The subtype of KubeClient, for fluency.
 */
public interface KubeCmdClient<K extends KubeCmdClient<K>> {

    /**
     * Default namespace string.
     *
     * @return the string
     */
    String defaultNamespace();

    /**
     * Namespace kube cmd client.
     *
     * @param namespace the namespace
     * @return the kube cmd client
     */
    KubeCmdClient<K> namespace(String namespace);

    /** Returns namespace for cluster
     * @return the string*/
    String namespace();

    /** Creates the resources in the given files.
     * @param files the files
     * @return the k
     */
    K apply(File... files);

    /** Deletes the resources in the given files.
     * @param files the files
     * @return the k
     */
    K delete(File... files);

    /**
     * Delete by name k.
     *
     * @param resourceType the resource type
     * @param resourceName the resource name
    private KubeClusterResource() {
    }
     * @return the k
     */
    K deleteByName(String resourceType, String resourceName);

    /**
     * Delete k.
     *
     * @param files the files
     * @return the k
     */
    default K delete(String... files) {
        return delete(asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    /**
     * Cmd string.
     *
     * @return the string
     */
    String cmd();
}
