/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.doxylicious.model.ProcDecl;
import io.kroxylicious.doxylicious.model.StepDecl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BaseTest {

    @Test
    void shouldRejectMissingSatisfies() {
        List<ProcDecl> procDecls = List.of(new ProcDecl("a", false, "b", List.of(), List.of()));

        assertThatThrownBy(() -> Base.from(procDecls)).hasMessage("proc 'a' has unknown proc 'b' in its satisfies");
    }

    @Test
    void shouldRejectMissingPrereq() {
        List<ProcDecl> procDecls = List.of(new ProcDecl("a", false, null, List.of("b"), List.of()));

        assertThatThrownBy(() -> Base.from(procDecls)).hasMessage("proc 'a' has unknown proc 'b' in its prereqs");
    }

    @Test
    void shouldRejectNotionalWithSteps() {
        List<ProcDecl> procDecls = List.of(
                new ProcDecl("a", true, null, List.of(), List.of(new StepDecl(null, "true"))));

        assertThatThrownBy(() -> Base.from(procDecls)).hasMessage("proc 'a' is notional, but has steps");
    }

    @Test
    void shouldRejectSatisfiesSatisfiesDirectRecursion() {
        List<ProcDecl> direct = List.of(
                new ProcDecl("a", false, "a", List.of(), List.of()));

        assertThatThrownBy(() -> Base.from(direct)).hasMessage("proc 'a' has a cyclic definition: a->satisfies->a");
    }

    @Test
    void shouldRejectSatisfiesSatisfiesIndirectRecursion() {
        List<ProcDecl> indirect = List.of(
                new ProcDecl("a", false, "b", List.of(), List.of()),
                new ProcDecl("b", false, "a", List.of(), List.of()));
        assertThatThrownBy(() -> Base.from(indirect)).hasMessage("proc 'b' has a cyclic definition: a->satisfies->b->satisfies->a");
    }

    @Test
    void shouldRejectPrereqDirectRecursion() {
        List<ProcDecl> direct = List.of(
                new ProcDecl("a", false, null, List.of("a"), List.of()));

        assertThatThrownBy(() -> Base.from(direct)).hasMessage("proc 'a' has a cyclic definition: a->prereq->a");
    }

    @Test
    void shouldRejectPrereqSatisfiesIndirectRecursion() {
        List<ProcDecl> indirect = List.of(
                new ProcDecl("a", false, null, List.of("b"), List.of()),
                new ProcDecl("b", false, "a", List.of(), List.of()));

        assertThatThrownBy(() -> Base.from(indirect)).hasMessage("proc 'b' has a cyclic definition: a->prereq->b->satisfies->a");
    }

    @Test
    void shouldRejectSatisfiesPrereqIndirectRecursion() {
        List<ProcDecl> indirect = List.of(
                new ProcDecl("a", false, "b", List.of(), List.of()),
                new ProcDecl("b", false, null, List.of("a"), List.of()));

        assertThatThrownBy(() -> Base.from(indirect)).hasMessage("proc 'b' has a cyclic definition: a->satisfies->b->prereq->a");
    }

    @Test
    void satisfiersOfConcreteProcIsEmpty() {
        List<ProcDecl> procDecls = List.of(new ProcDecl("a", false, null, List.of(), List.of()));
        Base base = Base.from(procDecls);
        assertThat(base.directSatisfiers("a")).isEmpty();
    }

    @Test
    void zeroSatisfiersOfAbstractProcIsEmpty() {
        List<ProcDecl> procDecls = List.of(new ProcDecl("x", true, null, List.of(), List.of()));
        Base base = Base.from(procDecls);
        assertThat(base.directSatisfiers("x")).isEmpty();
    }

    @Test
    void oneDirectConcreteSatisfierOfAbstractProc() {
        Base base = Base.from(List.of(
                new ProcDecl("x", true, null, List.of(), List.of()),
                new ProcDecl("a", false, "x", List.of(), List.of())));
        assertThat(base.directSatisfiers("x")).isEqualTo(Set.of("a"));
    }

    @Test
    void twoDirectConcreteSatisfierOfAbstractProc() {
        Base base = Base.from(List.of(
                new ProcDecl("x", true, null, List.of(), List.of()),
                new ProcDecl("a", false, "x", List.of(), List.of()),
                new ProcDecl("b", false, "x", List.of(), List.of())));
        assertThat(base.directSatisfiers("x")).isEqualTo(Set.of("a", "b"));
    }

    @Test
    void oneIndirectSatisfiersOfAbstractProc() {
        Base base = Base.from(List.of(
                new ProcDecl("x", true, null, List.of(), List.of()),
                new ProcDecl("a", true, "x", List.of(), List.of()),
                new ProcDecl("d", false, "a", List.of(), List.of())));
        assertThat(base.directSatisfiers("x")).isEqualTo(Set.of("a"));
        assertThat(base.directSatisfiers("a")).isEqualTo(Set.of("d"));
        assertThat(base.transitiveSatisfiers("x")).isEqualTo(Set.of("d"));
        assertThat(base.transitiveSatisfiers("a")).isEqualTo(Set.of("d"));

    }

}
