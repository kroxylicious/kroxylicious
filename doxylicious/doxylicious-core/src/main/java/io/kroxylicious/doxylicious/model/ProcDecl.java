/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Declares, possible indirectly, a sequence of steps to be performed.
 * @param id The name of this proc
 * @param notional Whether this proc is <em>notional</em>. A notional proc does not directly define any steps itself.
 * @param satisfies The name of the notional proc than this proc <em>satisfies</em>. This proc can be substituted for any proc that it satisfies.
 * @param prereqs The name of any procs which must be performed before this one.
 * @param procedure The steps of the procedure.
 * @param verification conditions which must hold after the steps in this proc have been completed.
 */
public record ProcDecl(
                       @JsonProperty(required = true) String id,
                       @JsonProperty(required = false) boolean notional,
                       @JsonProperty(required = false) @Nullable DocContent title,
                       @JsonProperty(value = "abstract", required = false) @Nullable DocContent abstract_,
                       @JsonProperty(required = false) @Nullable DocContent intro,
                       @JsonProperty(required = false) @Nullable String satisfies,
                       @JsonSetter(nulls = Nulls.AS_EMPTY) List<Prereq> prereqs,
                       @JsonSetter(nulls = Nulls.AS_EMPTY) List<StepDecl> procedure,
                       @JsonSetter(nulls = Nulls.AS_EMPTY) List<StepDecl> verification,
                       @JsonSetter(nulls = Nulls.AS_EMPTY) List<StepDecl> tearDown,
                       @JsonProperty(required = false) @Nullable DocContent additionalResources) {

    public ProcDecl(
                    String proc,
                    boolean notional,
                    @Nullable String satisfies,
                    List<String> prepreqs,
                    List<StepDecl> steps) {
        this(proc,
                notional,
                null,
                null,
                null,
                satisfies,
                prepreqs.stream().map(ref -> new Prereq("", ref)).toList(),
                steps,
                List.of(),
                List.of(),
                null);
    }

}
