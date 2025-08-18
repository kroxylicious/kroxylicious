/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package org.kroxylicious.doctools.asciidoc;

import java.nio.file.Path;

public record Block(Path asciiDocFile, int lineNumber, String content) {}
