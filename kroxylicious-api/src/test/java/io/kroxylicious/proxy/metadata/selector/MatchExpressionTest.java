/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata.selector;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MatchExpressionTest {

    @Test
    void existsWithNonNullValues() {
        assertThatThrownBy(() -> new MatchExpression("foo", MatchOperator.EXISTS, Set.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void notExistsWithNonNullValues() {
        assertThatThrownBy(() -> new MatchExpression("foo", MatchOperator.NOT_EXISTS, Set.of()))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void inWithNullValues() {
        assertThatThrownBy(() -> new MatchExpression("foo", MatchOperator.IN, null))
                .isExactlyInstanceOf(NullPointerException.class);
    }

    @Test
    void notInWithNullValues() {
        assertThatThrownBy(() -> new MatchExpression("foo", MatchOperator.NOT_IN, null))
                .isExactlyInstanceOf(NullPointerException.class);
    }

    @Test
    void existsToString() {
        assertThat(MatchExpression.exists("foo")).hasToString("foo");
    }

    @Test
    void notExistsToString() {
        assertThat(MatchExpression.notExists("foo")).hasToString("!foo");
    }

    @Test
    void inToString() {
        assertThat(MatchExpression.in("foo", Set.of("x"))).hasToString("foo==x");
        assertThat(MatchExpression.in("foo", new TreeSet<>(Set.of("x", "y")))).hasToString("foo in (x,y)");
    }

    @Test
    void notInToString() {
        assertThat(MatchExpression.notIn("foo", Set.of("x"))).hasToString("foo!=x");
        assertThat(MatchExpression.notIn("foo", new TreeSet<>(Set.of("x", "y")))).hasToString("foo notin (x,y)");
    }

    @Test
    void existsTest() {
        MatchExpression foo = MatchExpression.exists("foo");
        assertThat(foo.test(Map.of("foo", ""))).isTrue();
        assertThat(foo.test(Map.of("foo", "x"))).isTrue();
        assertThat(foo.test(Map.of("foo", "x", "bar", "y"))).isTrue();

        assertThat(foo.test(Map.of("bar", "y"))).isFalse();
        assertThat(foo.test(Map.of())).isFalse();
    }

    @Test
    void notExistsTest() {
        MatchExpression foo = MatchExpression.notExists("foo");
        assertThat(foo.test(Map.of("foo", ""))).isFalse();
        assertThat(foo.test(Map.of("foo", "x"))).isFalse();
        assertThat(foo.test(Map.of("foo", "x", "bar", "y"))).isFalse();

        assertThat(foo.test(Map.of("bar", "y"))).isTrue();
        assertThat(foo.test(Map.of())).isTrue();
    }

    @Test
    void inTest() {
        MatchExpression foo = MatchExpression.in("foo", Set.of("x", "y"));
        assertThat(foo.test(Map.of("foo", ""))).isFalse();
        assertThat(foo.test(Map.of("foo", "a"))).isFalse();
        assertThat(foo.test(Map.of("foo", "x"))).isTrue();
        assertThat(foo.test(Map.of("foo", "y"))).isTrue();
        assertThat(foo.test(Map.of("foo", "x", "bar", "y"))).isTrue();
        assertThat(foo.test(Map.of("foo", "y", "bar", "y"))).isTrue();

        assertThat(foo.test(Map.of("bar", "y"))).isFalse();
        assertThat(foo.test(Map.of())).isFalse();
    }

    @Test
    void notInTest() {
        MatchExpression foo = MatchExpression.notIn("foo", Set.of("x", "y"));
        assertThat(foo.test(Map.of("foo", ""))).isTrue();
        assertThat(foo.test(Map.of("foo", "a"))).isTrue();
        assertThat(foo.test(Map.of("foo", "x"))).isFalse();
        assertThat(foo.test(Map.of("foo", "y"))).isFalse();
        assertThat(foo.test(Map.of("foo", "x", "bar", "y"))).isFalse();
        assertThat(foo.test(Map.of("foo", "y", "bar", "y"))).isFalse();

        assertThat(foo.test(Map.of("bar", "y"))).isTrue();
        assertThat(foo.test(Map.of())).isTrue();
    }

    @Test
    void disjoint() {
        MatchExpression fooExists = MatchExpression.exists("foo");
        MatchExpression barExists = MatchExpression.exists("bar");
        MatchExpression fooNotExists = MatchExpression.notExists("foo");
        MatchExpression barNotExists = MatchExpression.notExists("bar");
        MatchExpression fooInXY = MatchExpression.in("foo", Set.of("x", "y"));
        MatchExpression fooInAB = MatchExpression.in("foo", Set.of("a", "b"));
        MatchExpression fooInAX = MatchExpression.in("foo", Set.of("a", "x"));
        MatchExpression barInXY = MatchExpression.in("bar", Set.of("x", "y"));
        MatchExpression fooNotInXY = MatchExpression.notIn("foo", Set.of("x", "y"));
        MatchExpression fooNotInXYZ = MatchExpression.notIn("foo", Set.of("x", "y", "z"));
        MatchExpression fooNotInYZ = MatchExpression.notIn("foo", Set.of("y", "z"));
        MatchExpression barNotInXY = MatchExpression.notIn("bar", Set.of("x", "y"));
        MatchExpression barNotInYZ = MatchExpression.notIn("bar", Set.of("y", "z"));

        // exists, notexists
        assertThat(fooExists.disjoint(fooNotExists)).isTrue();
        assertThat(barExists.disjoint(barNotExists)).isTrue();
        assertThat(fooExists.disjoint(barNotExists)).isFalse();
        // notexists, exists (the above, flipped)
        assertThat(fooNotExists.disjoint(fooExists)).isTrue();
        assertThat(barNotExists.disjoint(barExists)).isTrue();
        assertThat(barNotExists.disjoint(fooExists)).isFalse();

        // in, notexists
        assertThat(fooInXY.disjoint(fooNotExists)).isTrue();
        assertThat(barInXY.disjoint(barNotExists)).isTrue();
        assertThat(barInXY.disjoint(fooNotExists)).isFalse();
        // notexists, in (the above, flipped)
        assertThat(fooNotExists.disjoint(fooInXY)).isTrue();
        assertThat(barNotExists.disjoint(barInXY)).isTrue();
        assertThat(fooNotExists.disjoint(barInXY)).isFalse();

        // in, in
        assertThat(fooInXY.disjoint(fooInAB)).isTrue();
        assertThat(fooInXY.disjoint(fooInAX)).isFalse();
        assertThat(fooInXY.disjoint(barInXY)).isFalse();
        // in, in (the above, flipped)
        assertThat(fooInAB.disjoint(fooInXY)).isTrue();
        assertThat(fooInAX.disjoint(fooInXY)).isFalse();
        assertThat(barInXY.disjoint(fooInXY)).isFalse();

        // in, notin
        assertThat(fooInXY.disjoint(fooNotInXY)).isTrue();
        assertThat(fooInXY.disjoint(fooNotInXYZ)).isTrue();
        assertThat(fooInXY.disjoint(fooNotInYZ)).isFalse(); // because x is in both
        assertThat(fooInXY.disjoint(barNotInXY)).isFalse();
        assertThat(barInXY.disjoint(fooNotInXY)).isFalse();
        // notin, in (the above, flipped)
        assertThat(fooNotInXY.disjoint(fooInXY)).isTrue();
        assertThat(fooNotInXYZ.disjoint(fooInXY)).isTrue();
        assertThat(fooNotInYZ.disjoint(fooInXY)).isFalse(); // because x is in both
        assertThat(barNotInXY.disjoint(fooInXY)).isFalse();
        assertThat(fooNotInXY.disjoint(barInXY)).isFalse();
    }

}
