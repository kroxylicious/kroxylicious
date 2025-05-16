/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.exec;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.doxylicious.Base;
import io.kroxylicious.doxylicious.model.ProcDecl;
import io.kroxylicious.doxylicious.model.StepDecl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcExecutorTest {

    @Test
    void shouldThrowExceptionWhenProcessReturnsNonZeroExitValue() {

        StepDecl falseStep = new StepDecl(null, "false");
        var proc = new ProcDecl("fail", false, null, List.of(), List.of(falseStep));

        ProcExecutor procExecutor = new ProcExecutor(Base.from(List.of(proc)));
        assertThatThrownBy(() -> procExecutor.executeProcedure(proc))
                .isExactlyInstanceOf(ExecException.class)
                .hasMessage("'fail/procedure/0/exec': Command [false] failed with code 1 and no error output");
    }

    @Test
    void shouldNotThrowExceptionWhenProcessReturnsZeroExitValue() {
        StepDecl trueStep = new StepDecl(null, "true");
        var proc = new ProcDecl("succeed", false, null, List.of(), List.of(trueStep));
        ProcExecutor procExecutor = new ProcExecutor(Base.from(List.of(proc)));
        assertThatCode(() -> procExecutor.executeProcedure(proc)).doesNotThrowAnyException();
    }

    @Test
    void shouldThrowWhenTimeoutExpires() {
        StepDecl sleepStep = new StepDecl(null, "sleep 10");
        var proc = new ProcDecl("sleep_for_a_bit", false, null, List.of(), List.of(sleepStep));

        ProcExecutor procExecutor = new ProcExecutor(Base.from(List.of(proc)), null, Duration.ofMillis(100), Duration.ofSeconds(5));
        assertThatThrownBy(() -> procExecutor.executeProcedure(proc))
                .isExactlyInstanceOf(ExecException.class)
                .hasMessage("'sleep_for_a_bit/procedure/0/exec': Timed out waiting for command ['sleep', '10'] to exit (process destroyed)");
    }

    @Test
    void shouldCountA() {
        ProcDecl a = new ProcDecl("a", false, null, List.of(), List.of());
        ProcExecutor procExecutor = new ProcExecutor(Base.from(List.of(a)));
        assertThat(procExecutor.suite("a").count()).isOne();
    }

    @Test
    void shouldCountDiamond() {
        ProcDecl a = new ProcDecl("a", false, null, List.of("d", "e"), List.of());
        ProcDecl d = new ProcDecl("d", false, null, List.of("f"), List.of());
        ProcDecl e = new ProcDecl("e", false, null, List.of("f"), List.of());
        ProcDecl f = new ProcDecl("f", false, null, List.of(), List.of());
        var base = Base.from(List.of(a, d, e, f));
        ProcExecutor procExecutor = new ProcExecutor(base);
        assertThat(procExecutor.suite("a").count()).isOne();
        assertThat(procExecutor.suite("a").describe()).isEqualTo("f d e a");
        assertThat(procExecutor.suite("d").describe()).isEqualTo("f d");
        assertThat(procExecutor.suite("e").describe()).isEqualTo("f e");
        assertThat(procExecutor.suite("f").describe()).isEqualTo("f");
    }

    @Test
    void shouldExpandAbstractPrereq() {
        ProcDecl a = new ProcDecl("a", false, null, List.of("b"), List.of());
        ProcDecl b = new ProcDecl("b", true, null, List.of(), List.of());
        ProcDecl c = new ProcDecl("c", false, "b", List.of(), List.of());
        ProcDecl d = new ProcDecl("d", false, "b", List.of(), List.of());
        var base = Base.from(List.of(a, b, c, d));
        ProcExecutor procExecutor = new ProcExecutor(base);
        assertThat(procExecutor.suite("a").count()).isEqualTo(2);
        assertThat(procExecutor.suite("a").describe()).isEqualTo("""
                c a
                d a""");

        assertThat(procExecutor.suite("a", Set.of("a"), Map.of()).describe())
                .describedAs("a ASSUMING a should be empty")
                .isEqualTo("");

        assertThat(procExecutor.suite("a", Set.of("c", "d"), Map.of()).describe()).isEqualTo("""
                a""");

        assertThat(procExecutor.suite("a", Set.of("b"), Map.of()).describe()).isEqualTo("""
                a""");

        assertThat(procExecutor.suite("a", Set.of("c"), Map.of()).describe()).isEqualTo("""
                a
                d a""");

        assertThat(procExecutor.suite("a", Set.of(), Map.of("b", "c")).describe()).isEqualTo("""
                c a""");

    }

    @Test
    void shouldCountTwoSatisfiersOfOneAbstract() {
        ProcDecl a = new ProcDecl("a", true, null, List.of(), List.of());
        ProcDecl b = new ProcDecl("b", false, "a", List.of(), List.of());
        ProcDecl c = new ProcDecl("c", false, "a", List.of(), List.of());
        ProcDecl d = new ProcDecl("d", false, null, List.of("a"), List.of());
        var base = Base.from(List.of(a, b, c, d));
        ProcExecutor procExecutor = new ProcExecutor(base);
        assertThat(procExecutor.suite("a").count()).isEqualTo(2);
        assertThat(procExecutor.suite("a").describe()).isEqualTo("""
                b
                c""");
        assertThat(procExecutor.suite("b").count()).isOne();
        assertThat(procExecutor.suite("b").describe()).isEqualTo("""
                b""");
        assertThat(procExecutor.suite("c").count()).isOne();
        assertThat(procExecutor.suite("c").describe()).isEqualTo("""
                c""");
        assertThat(procExecutor.suite("d").count()).isEqualTo(2);
        assertThat(procExecutor.suite("d").describe()).isEqualTo("""
                b d
                c d""");
    }

}
