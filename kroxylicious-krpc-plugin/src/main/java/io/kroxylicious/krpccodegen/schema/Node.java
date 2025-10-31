/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.util.List;

public record Node(List<FieldSpec> entities, List<Node> containers, List<Short> orderedVersions) {
    public boolean hasAtLeastOneEntityField() {
        return !(entities.isEmpty() && containers.isEmpty());
    }

}
