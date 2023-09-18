/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.k8s.cmd;

import java.io.File;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * Abstraction for a kubernetes client.
 * @param <K> The subtype of KubeClient, for fluency.
 */
public interface KubeCmdClient<K extends KubeCmdClient<K>> {

    String defaultNamespace();

    KubeCmdClient<K> namespace(String namespace);

    /** Returns namespace for cluster */
    String namespace();

    /** Creates the resources in the given files. */
    K apply(File... files);

    /** Deletes the resources in the given files. */
    K delete(File... files);

    K deleteByName(String resourceType, String resourceName);

    default K delete(String... files) {
        return delete(asList(files).stream().map(File::new).collect(toList()).toArray(new File[0]));
    }

    String cmd();
}
