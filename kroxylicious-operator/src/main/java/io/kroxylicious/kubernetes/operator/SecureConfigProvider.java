/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.nio.file.Path;

/**
 * A secure config provider is able to provide references to files to be provided within the container's
 * VFS which will be referenced from the proxy config file.
 */
public interface SecureConfigProvider {

    /**
     * Returns container file information for a given {@code ${<name>:<path>:<key>}} interpolation placeholder.
     * The interpretation of {@code <path>} and {@code key} is provider dependent.
     * @param name The name for this provider (from the placeholder)
     * @param path The path (from the placeholder)
     * @param key The key (from the placeholder)
     * @param mountPathBase The mount path base.
     * @return a container file
     */
    ContainerFileReference containerFile(String name, String path, String key, Path mountPathBase);

}
