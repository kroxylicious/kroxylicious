/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.exec;

import java.util.List;
import java.util.stream.Collectors;

import io.kroxylicious.doxylicious.model.ProcDecl;

public record Suite(String name, List<List<ProcDecl>> cases) {

    public long count() {
        return cases.size();
    }

    public String describe() {
        return cases.stream()
                .map(case_ -> case_.stream()
                        .map(ProcDecl::id)
                        .collect(Collectors.joining(" ")))
                .collect(Collectors.joining("\n"));
    }

}
