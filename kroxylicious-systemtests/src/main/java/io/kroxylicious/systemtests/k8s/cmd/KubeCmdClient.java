/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.k8s.cmd;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import io.kroxylicious.systemtests.executor.ExecResult;

import static java.util.Arrays.asList;

/**
 * Abstraction for a kubernetes client.
 * @param <K> The subtype of KubeClient, for fluency.
 */
public interface KubeCmdClient<K extends KubeCmdClient<K>> {

    /**
     * Default namespace string.
     *
     * @return the string
     */
    String defaultNamespace();

    /**
     * kube cmd client instance with the namespace indicated.
     *
     * @param namespace the namespace
     * @return the kube cmd client instance
     */
    KubeCmdClient<K> getInstanceWithNamespace(String namespace);

    /** Returns namespace for cluster
     * @return the string
     */
    String getNamespace();

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
     * @return the k
     */
    K deleteByName(String resourceType, String resourceName);

    K deleteAllByResource(String resourceType);

    /**
     * Delete k.
     *
     * @param files the files
     * @return the k
     */
    default K delete(String... files) {
        return delete(Arrays.stream(files).map(File::new).toArray(File[]::new));
    }

    /**
     * Cmd string.
     *
     * @return the string
     */
    String cmd();

    /**
     * Exec in pod.
     *
     * @param pod the pod
     * @param throwErrors the throw errors
     * @param command the command
     * @return the exec result
     */
    default ExecResult execInPod(String pod, boolean throwErrors, String... command) {
        return execInPod(pod, throwErrors, asList(command));
    }

    /**
     * Exec in pod.
     *
     * @param pod the pod
     * @param throwErrors the throw errors
     * @param command the command
     * @return the exec result
     */
    ExecResult execInPod(String pod, boolean throwErrors, List<String> command);
}
