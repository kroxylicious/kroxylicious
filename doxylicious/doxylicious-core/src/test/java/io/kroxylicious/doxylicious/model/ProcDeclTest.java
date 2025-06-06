/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.model;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import static org.assertj.core.api.Assertions.assertThat;

class ProcDeclTest {

    @Test
    void shouldDeserialize() throws IOException {
        var proc = new YAMLMapper().readValue(getClass().getResource("/example_proc.yaml"), ProcDecl.class);

        assertThat(proc.id()).isEqualTo("example_proc");
        assertThat(proc.notional()).isFalse();
        assertThat(proc.satisfies()).isEqualTo("another_proc");
        assertThat(proc.title().adoc()).isEqualTo("Deploying a minimal proxy instance");
        assertThat(proc.abstract_().adoc()).isEqualTo("This procedure shows how to deploy a basic Kroxylicious proxy, without any filters, "
                + "accessible using plain TCP networking to clients in the same Kubernetes cluster.");
        assertThat(proc.intro().adoc()).isEqualTo("Blah blah");
        assertThat(proc.prereqs()).isEqualTo(List.of(new Prereq("Access to a Kubernetes cluster using `kubectl`", "have_a_kubectl")));
        assertThat(proc.procedure()).isEqualTo(List.of(
                new StepDecl(null, null, List.of(
                        new StepDecl("Install the examples", null),
                        new StepDecl(null, new ExecDecl(null, null, "kubectl apply -f examples/simple/", null, null, null, null, null, null,
                                new ExecDecl("kubectl delete -f examples/simple/", null, null, null, null, null, null)), null),
                        new StepDecl("That ought to do it", null))),
                new StepDecl(null, null, List.of(
                        new StepDecl("Drink some coffee", null)))));
        assertThat(proc.additionalResources().adoc().trim()).isEqualTo("""
                * additional resource 1
                * additional resource 2""");
    }

}
