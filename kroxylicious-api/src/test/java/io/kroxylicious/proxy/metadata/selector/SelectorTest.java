/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata.selector;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SelectorTest {

    @Test
    void parseEquality() {
        assertThat(Selector.parse("environment = production").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("environment", MatchOperator.IN, Set.of("production"))));
        assertThat(Selector.parse("example.org/environment = production").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("example.org/environment", MatchOperator.IN, Set.of("production"))));
        assertThat(Selector.parse("environment==production").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("environment", MatchOperator.IN, Set.of("production"))));
        assertThat(Selector.parse("tier != frontend").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("tier", MatchOperator.NOT_IN, Set.of("frontend"))));
        assertThat(Selector.parse("example.org/tier != frontend").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("example.org/tier", MatchOperator.NOT_IN, Set.of("frontend"))));

        assertThat(Selector.parse("environment == production, tier!=frontend").matchExpressions())
                .isEqualTo(List.of(
                        new MatchExpression("environment", MatchOperator.IN, Set.of("production")),
                        new MatchExpression("tier", MatchOperator.NOT_IN, Set.of("frontend"))));

        // label values can be empty
        assertThat(Selector.parse("environment = ").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("environment", MatchOperator.IN, Set.of(""))));
        assertThat(Selector.parse("example.org/environment = ").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("example.org/environment", MatchOperator.IN, Set.of(""))));

        // 'in' can be used in a label value
        assertThat(Selector.parse("country = in").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("country", MatchOperator.IN, Set.of("in"))));

        // 'in' can be used in a label key
        assertThat(Selector.parse("in = true").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("in", MatchOperator.IN, Set.of("true"))));
        assertThat(Selector.parse("in/in = true").matchExpressions())
                .isEqualTo(List.of(new MatchExpression("in/in", MatchOperator.IN, Set.of("true"))));
    }

    @Test
    void parseSetBased() {
        assertThat(Selector.parse("environment in (production, qa)")).isEqualTo(
                Selector.compile(List.of(new MatchExpression("environment", MatchOperator.IN, Set.of("production", "qa")))));
        assertThat(Selector.parse("tier notin (frontend, backend)")).isEqualTo(
                Selector.compile(List.of(new MatchExpression("tier", MatchOperator.NOT_IN, Set.of("frontend", "backend")))));

        // label value can be empty
        assertThat(Selector.parse("environment in ()")).isEqualTo(
                Selector.compile(List.of(new MatchExpression("environment", MatchOperator.IN, Set.of("")))));

        // 'in' can be used in a label value
        assertThat(Selector.parse("country in (in)")).isEqualTo(
                Selector.compile(List.of(new MatchExpression("country", MatchOperator.IN, Set.of("in")))));

        // 'in' can be used in a label key
        assertThat(Selector.parse("in in (in)")).isEqualTo(
                Selector.compile(List.of(new MatchExpression("in", MatchOperator.IN, Set.of("in")))));

        assertThat(Selector.parse("notin notin (notin)")).isEqualTo(
                Selector.compile(List.of(new MatchExpression("notin", MatchOperator.NOT_IN, Set.of("notin")))));
    }

    @Test
    void parseExists() {
        assertThat(Selector.parse("partition")).isEqualTo(Selector.compile(List.of(new MatchExpression("partition", MatchOperator.EXISTS, null))));
        assertThat(Selector.parse("org.example/partition"))
                .isEqualTo(Selector.compile(List.of(new MatchExpression("org.example/partition", MatchOperator.EXISTS, null))));
        assertThat(Selector.parse("!partition")).isEqualTo(Selector.compile(List.of(new MatchExpression("partition", MatchOperator.NOT_EXISTS, null))));
        assertThat(Selector.parse("!org.example/partition"))
                .isEqualTo(Selector.compile(List.of(new MatchExpression("org.example/partition", MatchOperator.NOT_EXISTS, null))));

        // 'in' can be used in a label key
        assertThat(Selector.parse("in")).isEqualTo(Selector.compile(List.of(new MatchExpression("in", MatchOperator.EXISTS, null))));
        assertThat(Selector.parse("org.example/in")).isEqualTo(Selector.compile(List.of(new MatchExpression("org.example/in", MatchOperator.EXISTS, null))));
        assertThat(Selector.parse("in/in")).isEqualTo(Selector.compile(List.of(new MatchExpression("in/in", MatchOperator.EXISTS, null))));
    }

    @Test
    void parseError() {
        assertThatThrownBy(() -> Selector.parse("environment = = production"))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("invalid selector syntax at 1:14: no viable alternative at input '='");

        assertThatThrownBy(() -> Selector.parse(""))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("invalid selector syntax at 1:0: mismatched input '<EOF>' expecting {'in', 'notin', '!', LETTER, DIGIT}");

        assertThatThrownBy(() -> Selector.parse("!"))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("invalid selector syntax at 1:1: mismatched input '<EOF>' expecting {'in', 'notin', LETTER, DIGIT}");

        assertThatThrownBy(() -> Selector.parse("environment in"))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("invalid selector syntax at 1:14: mismatched input '<EOF>' expecting '('");

        assertThatThrownBy(() -> Selector.parse("environment notin"))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("invalid selector syntax at 1:17: mismatched input '<EOF>' expecting '('");

        assertThatThrownBy(() -> Selector.parse("environment in ("))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("invalid selector syntax at 1:16: mismatched input '<EOF>' expecting {',', ')'}");

        assertThatThrownBy(() -> Selector.parse("foo/bar/baz"))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("invalid selector syntax at 1:7: mismatched input '/' expecting {<EOF>, ','}");

        String tooLongValue = "a=" + "x".repeat(64);
        assertThatThrownBy(() -> Selector.parse(tooLongValue))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("value is too long: must be 63 characters or fewer");

        String tooLongName = "x".repeat(64);
        assertThatThrownBy(() -> Selector.parse(tooLongName))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("name in prefix is too long: must be 63 characters or fewer");

        String tooLongPrefix = "x".repeat(254) + "/y";
        assertThatThrownBy(() -> Selector.parse(tooLongPrefix))
                .isExactlyInstanceOf(LabelException.class)
                .hasMessage("DNS subdomain in prefix is too long: must be 253 characters or fewer");
    }

    @Test
    void testToString() {
        assertThat(Selector.compile(List.of(new MatchExpression("environment", MatchOperator.NOT_IN, Set.of("production")))))
                .hasToString("environment!=production");
        assertThat(Selector.compile(List.of(new MatchExpression("environment", MatchOperator.IN, Set.of("production")))))
                .hasToString("environment==production");
        assertThat(Selector.compile(List.of(new MatchExpression("environment", MatchOperator.IN, Set.of("production", "dev")))))
                .hasToString("environment in (dev,production)");
        assertThat(Selector.compile(List.of(new MatchExpression("environment", MatchOperator.NOT_IN, Set.of("production", "dev")))))
                .hasToString("environment notin (dev,production)");
        assertThat(Selector.compile(List.of(new MatchExpression("environment", MatchOperator.IN, Set.of()))))
                .hasToString("environment in ()");

        assertThat(Selector.compile(List.of(new MatchExpression("environment", MatchOperator.EXISTS, null))))
                .hasToString("environment");
        assertThat(Selector.compile(List.of(new MatchExpression("environment", MatchOperator.NOT_EXISTS, null))))
                .hasToString("!environment");
    }

    @Test
    void simplification() {
        // TODO call canonicalise directly, now it's a separate method?
        assertThat(Selector.parse("foo,foo")).hasToString("foo");
        assertThat(Selector.parse("!foo,!foo")).hasToString("!foo");
        assertThat(Selector.parse("foo,!foo").matchesNothing()).isTrue(); // don't have a good way to say NONE
        assertThat(Selector.parse("foo in (x, y), foo in (x, z)")).hasToString("foo==x");
        assertThat(Selector.parse("foo notin (x, y), foo notin (x, z)")).hasToString("foo notin (x,y,z)");

        assertThat(Selector.parse("foo, foo in (x, z)")).hasToString("foo in (x,z)");
        assertThat(Selector.parse("!foo, foo in (x, z)").matchesNothing()).isTrue(); // don't have a good way to say NONE
        assertThat(Selector.parse("foo, foo notin (x, z)")).hasToString("foo,foo notin (x,z)");
        assertThat(Selector.parse("!foo, foo notin (x, z)")).hasToString("!foo");
        assertThat(Selector.parse("foo in (x, y), foo notin (y, z)")).hasToString("foo==x");

    }

    @Test
    void testExists() {
        assertThat(Selector.parse("environment").test(Map.of("environment", "production"))).isTrue();
        assertThat(Selector.parse("environment").test(Map.of("tier", "frontend"))).isFalse();
        assertThat(Selector.parse("!environment").test(Map.of("tier", "frontend"))).isTrue();
    }

    @Test
    void testEquality() {
        assertThat(Selector.parse("environment=production").test(Map.of("environment", "production"))).isTrue();
        assertThat(Selector.parse("environment!=production").test(Map.of("environment", "production"))).isFalse();
        assertThat(Selector.parse("environment=dev").test(Map.of("environment", "production"))).isFalse();
        assertThat(Selector.parse("environment!=dev").test(Map.of("environment", "production"))).isTrue();
        assertThat(Selector.parse("environment=production").test(Map.of("tier", "frontend"))).isFalse();
        assertThat(Selector.parse("environment!=production").test(Map.of("tier", "frontend"))).isTrue();
    }

    @Test
    void testSetBased() {
        assertThat(Selector.parse("environment in (production, dev)").test(Map.of("environment", "production"))).isTrue();
        assertThat(Selector.parse("environment in (production, dev)").test(Map.of("environment", "dev"))).isTrue();
        assertThat(Selector.parse("environment in (production, dev)").test(Map.of("environment", "stage"))).isFalse();
        assertThat(Selector.parse("environment in (production, dev)").test(Map.of("tier", "frontend"))).isFalse();

        assertThat(Selector.parse("environment notin (production, dev)").test(Map.of("environment", "stage"))).isTrue();
        assertThat(Selector.parse("environment notin (production, dev)").test(Map.of("tier", "frontend"))).isTrue();

        assertThat(Selector.parse("environment in (production, dev),tier=frontend").test(Map.of("environment", "production"))).isFalse();
        assertThat(Selector.parse("environment in (production, dev),tier=frontend").test(Map.of("environment", "production",
                "tier", "frontend"))).isTrue();
    }

    @Test
    void disjoint() {
        assertThat(Selector.parse("foo").disjoint(Selector.parse("foo"))).isFalse();
        assertThat(Selector.parse("foo").disjoint(Selector.parse("!foo"))).isTrue();
        assertThat(Selector.parse("foo,bar").disjoint(Selector.parse("!foo"))).isTrue();
        assertThat(Selector.parse("foo").disjoint(Selector.parse("!foo,bar"))).isTrue();
        assertThat(Selector.parse("foo,bar").disjoint(Selector.parse("!foo,bar"))).isFalse();
        assertThat(Selector.parse("foo,!bar").disjoint(Selector.parse("!foo,bar"))).isTrue();

        assertThat(Selector.parse("foo in (x)").disjoint(Selector.parse("foo in (y)"))).isTrue();
        assertThat(Selector.parse("foo in (x, y)").disjoint(Selector.parse("foo in (y)"))).isFalse();
        assertThat(Selector.parse("foo in (x, y)").disjoint(Selector.parse("foo in (x)"))).isFalse();
        assertThat(Selector.parse("foo in (x, y)").disjoint(Selector.parse("foo in (x, y)"))).isFalse();
        assertThat(Selector.parse("foo in (x, y)").disjoint(Selector.parse("foo in (x, y, z)"))).isFalse();

        assertThat(Selector.parse("foo in (x)").disjoint(Selector.parse("!foo"))).isTrue();
        assertThat(Selector.parse("foo in ()").disjoint(Selector.parse("!foo"))).isTrue();

        assertThat(Selector.parse("foo in (x)").disjoint(Selector.parse("foo notin (y)"))).isFalse();
        assertThat(Selector.parse("foo notin (x)").disjoint(Selector.parse("foo notin (y)"))).isFalse();
    }

    private final static ObjectMapper MAPPER = new YAMLMapper();

    @Test
    void testDeserializationFromYaml() throws JsonProcessingException {
        Selector justMatchLabels = MAPPER.readValue("""
                matchLabels:
                  foo: x
                  bar: y
                """, Selector.class);
        assertThat(justMatchLabels.matchExpressions()).isEqualTo(List.of(
                new MatchExpression("bar", MatchOperator.IN, Set.of("y")),
                new MatchExpression("foo", MatchOperator.IN, Set.of("x"))));

        Selector justMatchExpressions = MAPPER.readValue("""
                matchExpressions:
                - key: foo
                  operator: In
                  values:
                  - x
                - key: bar
                  operator: NotIn
                  values:
                  - a
                  - b
                - key: baz
                  operator: Exists
                - key: quux
                  operator: DoesNotExist
                """, Selector.class);
        assertThat(justMatchExpressions.matchExpressions()).isEqualTo(List.of(
                new MatchExpression("bar", MatchOperator.NOT_IN, Set.of("a", "b")),
                new MatchExpression("baz", MatchOperator.EXISTS, null),
                new MatchExpression("foo", MatchOperator.IN, Set.of("x")),
                new MatchExpression("quux", MatchOperator.NOT_EXISTS, null)));

        Selector matchLabelsAndMatchExpressions = MAPPER.readValue("""
                matchLabels:
                  foo: x
                matchExpressions:
                - key: baz
                  operator: Exists
                """, Selector.class);
        assertThat(matchLabelsAndMatchExpressions.matchExpressions()).isEqualTo(List.of(
                new MatchExpression("baz", MatchOperator.EXISTS, null),
                new MatchExpression("foo", MatchOperator.IN, Set.of("x"))));

    }

    @Test
    void and() {
        Selector fooExists = Selector.parse("foo");
        Selector barExists = Selector.parse("bar");
        assertThat(fooExists.and(barExists).matchExpressions()).isEqualTo(List.of(
                new MatchExpression("bar", MatchOperator.EXISTS, null),
                new MatchExpression("foo", MatchOperator.EXISTS, null)));
        assertThat(fooExists.and(fooExists).matchExpressions()).isEqualTo(List.of(
                new MatchExpression("foo", MatchOperator.EXISTS, null)));
    }
    // TODO need test for equals(), and that equals <=> selects same labels
    // TODO decide whether a selector is thing expression, and we can get a SelectorPredicate from it is canonicalized and has the higher semantics we want
    // alternatively have a toString() which returns the canonicalised string version, and a selector() method which returns the string form of the original selector

}
