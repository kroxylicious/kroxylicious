/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import java.util.List;

/**
 * Represents an identifiable container of {@link ProcDecl}s (such as a file).
 * @param sourceName The name of the container (e.g. the file name)
 * @param procs The procs in this container.
 */
public record Unit(String sourceName, List<ProcDecl> procs) {}
