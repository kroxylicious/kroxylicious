/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.tools.crd;

import java.nio.file.Path;

import io.kroxylicious.tools.crd.model.Crd;

/**
 * A CRD, read from a file
 * @param src
 * @param crd
 */
public record InputCrd(Path src, Crd crd) {}
