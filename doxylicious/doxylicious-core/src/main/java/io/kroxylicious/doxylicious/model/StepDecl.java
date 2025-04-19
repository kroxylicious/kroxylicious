/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import edu.umd.cs.findbugs.annotations.Nullable;

public record StepDecl(
                       @JsonProperty(required = false) @Nullable String adoc,
                       @JsonProperty(required = false) @Nullable @JsonDeserialize(using = ExecDecl.Deserializer.class) ExecDecl exec,
                       @JsonProperty(value = "step", required = false) @Nullable List<StepDecl> substeps)
        implements Documented {

    static ExecDecl command(String command) {
        return new ExecDecl(command, null, null, null, null, null, null);
    }

    public StepDecl(@Nullable String adoc,
                    @Nullable String exec) {
        this(adoc, exec != null ? command(exec) : null, null);
    }

    public StepDecl {
        if ((adoc != null && (exec != null || substeps != null))
                || (exec != null && substeps != null)) {
            throw new IllegalArgumentException("Step can only have a single property specified");
        }
    }
}
