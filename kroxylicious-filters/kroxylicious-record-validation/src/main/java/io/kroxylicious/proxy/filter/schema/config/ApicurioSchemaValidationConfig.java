/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;

// This has an ugly equals/hash but in future we will definitely have configuration for the
// validation. The equals/hash are required to keep the test happy. We use the presence of
// this config object to enable checking.
public class ApicurioSchemaValidationConfig {

    /**
     * Construct ApicurioSchemaValidationConfig
     */
    @JsonCreator
    public ApicurioSchemaValidationConfig() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(1);
    }

    @Override
    public String toString() {
        return "Schema Validation";
    }
}
