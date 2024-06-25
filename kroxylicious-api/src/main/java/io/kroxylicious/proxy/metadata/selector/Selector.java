/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata.selector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>A predicate on labels (string-to-string maps), consisting of a number of {@linkplain MatchExpression expressions}
 * that must all match in order for the selector to match.
 * This is exactly the same as Kubernetes' selector.</p>
 */
public final class Selector implements Predicate<Map<String, String>> {

    public static final Selector ALL = new Selector(List.of());
    public static final Selector NONE = new Selector(List.of(
            new MatchExpression("x", MatchOperator.EXISTS, null),
            new MatchExpression("x", MatchOperator.NOT_EXISTS, null)));

    private final List<MatchExpression> matchExpressions;

    private Selector(List<MatchExpression> matchExpressions) {
        this.matchExpressions = matchExpressions;
    }

    public static Selector compile(List<MatchExpression> matchExpressions) {
        return canonicalize(matchExpressions);
    }

    /**
     * Canonicalizes the given {@code matchExpressions} returning a Selector that will match the same labels.
     * If the given expressions select all labels, or none, then those constant selectors will be returned
     * Otherwise this returns a new Selector whose's {@link #matchExpressions} will be in alphabetic order by key,
     * and each key having at most one {@link MatchExpression} for each {@link MatchOperator}
     *
     * The guarantees provided by this function mean that two selectors that are {@linkplain #equals(Object) equal} will match exactly
     * the same set of labels, and that {@link #toString()} will return a canonical result.
     */
    @SuppressWarnings("java:S125") // sonar gets confused by comments which aren't code but use { }
    private static Selector canonicalize(List<MatchExpression> matchExpressions) {
        var canonical = new ArrayList<MatchExpression>();
        // Group by same key, in key order
        var groupedByKey = matchExpressions.stream()
                .collect(Collectors.groupingBy(MatchExpression::key, TreeMap::new, Collectors.toList()));
        for (var entry : groupedByKey.entrySet()) {
            var key = entry.getKey();
            List<MatchExpression> expressions = entry.getValue();
            // Step 1. Simplify expressions which use the same operator
            var exprByOperator = expressions.stream().collect(Collectors.groupingBy(MatchExpression::operator));
            MatchExpression existsExpr = simplifyMultipleExists(exprByOperator);
            MatchExpression notExistsExpr = simplifyMultipleNotExists(exprByOperator);
            MatchExpression inExpr = simplifyMultipleIn(exprByOperator, key);
            MatchExpression notInExpr = simplifyMultipleNotIn(exprByOperator, key);

            // Step 2. Simplify different operations
            var localCanonical = simplifyExprs(existsExpr, notExistsExpr, inExpr, notInExpr, key);
            if (localCanonical == null) {
                // NOTE "()" means the set containing the empty value, {""}, not the empty set {}
                // foo in {} == NONE
                // foo notin {} == ALL
                return NONE;
            }
            canonical.addAll(localCanonical);

        }
        return new Selector(List.copyOf(canonical));
    }

    @SuppressWarnings("java:S125") // sonar gets confused by comments which aren't code but use { }
    private static @Nullable ArrayList<MatchExpression> simplifyExprs(MatchExpression existsExpr,
                                                                      MatchExpression notExistsExpr,
                                                                      MatchExpression inExpr,
                                                                      MatchExpression notInExpr,
                                                                      String key) {
        if (existsExpr != null) {
            if (notExistsExpr != null) {
                // EXIST, NOTEXISTS "foo, !foo" == NONE
                return null;
            }
            if (inExpr != null) {
                // EXISTS, IN "foo, foo in X" == "foo in X', so can drop the exists
                existsExpr = null;
            }
            // if (notInExpr != null) {
            // // EXISTS, NOTIN "foo, foo notin X" == Can't be simplified, because "foo notin X" selects things without foo
            // }
        }
        if (notExistsExpr != null) {
            if (inExpr != null) {
                // NOTEXISTS, IN "!foo, foo in X" == NONE
                return null;
            }
            if (notInExpr != null) {
                // NOTEXISTS, NOTIN "!foo, foo notin X" == !foo
                notInExpr = null;
            }
        }
        if (inExpr != null && notInExpr != null) {
            // IN, NOTIN "foo in X, foo notin Y" == "foo in X-Y"
            inExpr = new MatchExpression(key, MatchOperator.IN, difference(inExpr.nonNullValues(), notInExpr.nonNullValues()));
            notInExpr = null;
        }

        var localCanonical = new ArrayList<MatchExpression>();
        if (existsExpr != null) {
            localCanonical.add(existsExpr);
        }
        if (notExistsExpr != null) {
            localCanonical.add(notExistsExpr);
        }
        if (inExpr != null) {
            localCanonical.add(inExpr);
        }
        if (notInExpr != null) {
            localCanonical.add(notInExpr);
        }
        return localCanonical;
    }

    public boolean matchesNothing() {
        return this == NONE;
    }

    public boolean matchesEverything() {
        return this == ALL;
    }

    @Nullable
    private static MatchExpression simplifyMultipleNotIn(
                                                         Map<MatchOperator, List<MatchExpression>> exprByOperator,
                                                         String key) {
        List<MatchExpression> notInList = exprByOperator.getOrDefault(MatchOperator.NOT_IN, List.of());
        MatchExpression notInExpr;
        if (!notInList.isEmpty()) {
            // "foo notin X, foo notin Y" == "foo in union(X, Y)"
            var union = notInList.stream().map(MatchExpression::nonNullValues).reduce(Selector::union).orElse(Set.of());
            notInExpr = new MatchExpression(key, MatchOperator.NOT_IN, union);
        }
        else {
            notInExpr = null;
        }
        return notInExpr;
    }

    @Nullable
    private static MatchExpression simplifyMultipleIn(
                                                      Map<MatchOperator, List<MatchExpression>> exprByOperator,
                                                      String key) {
        List<MatchExpression> inList = exprByOperator.getOrDefault(MatchOperator.IN, List.of());
        MatchExpression inExpr;
        if (inList.size() > 1) {
            // "foo in X, foo in Y" == "foo in intersection(X, Y)"
            var intersection = inList.stream().map(MatchExpression::nonNullValues).reduce(Selector::intersection).orElse(Set.of());
            inExpr = new MatchExpression(key, MatchOperator.IN, intersection);
        }
        else if (inList.size() == 1) {
            inExpr = inList.get(0);
        }
        else {
            inExpr = null;
        }
        return inExpr;
    }

    @Nullable
    private static MatchExpression simplifyMultipleNotExists(Map<MatchOperator, List<MatchExpression>> exprByOperator) {
        MatchExpression notExistsExpr = null;
        if (exprByOperator.containsKey(MatchOperator.NOT_EXISTS)) {
            // "!foo, !foo" == "!foo"
            notExistsExpr = exprByOperator.get(MatchOperator.NOT_EXISTS).get(0);
        }
        return notExistsExpr;
    }

    @Nullable
    private static MatchExpression simplifyMultipleExists(Map<MatchOperator, List<MatchExpression>> exprByOperator) {
        MatchExpression existsExpr = null;
        if (exprByOperator.containsKey(MatchOperator.EXISTS)) {
            // "foo, foo" == "foo"
            existsExpr = exprByOperator.get(MatchOperator.EXISTS).get(0);
        }
        return existsExpr;
    }

    private static <T> HashSet<T> union(
                                        Set<T> s1,
                                        Set<T> s2) {
        var result = new HashSet<T>((int) Math.ceil((s1.size() + s2.size()) / 0.75));
        result.addAll(s1);
        result.addAll(s2);
        return result;
    }

    private static <T> HashSet<T> intersection(
                                               Set<T> s1,
                                               Set<T> s2) {
        var result = new HashSet<T>((int) Math.ceil(Math.min(s1.size(), s2.size()) / 0.75));
        result.addAll(s1);
        result.retainAll(s2);
        return result;
    }

    private static <T> @NonNull Set<T> difference(
                                                  @NonNull Set<T> s1,
                                                  @NonNull Set<T> s2) {
        var result = new HashSet<T>((int) Math.ceil(s1.size() / 0.75));
        result.addAll(s1);
        result.removeAll(s2);
        return result;
    }

    @JsonCreator
    public static Selector compile(
                                   @JsonProperty("matchLabels") Map<String, String> matchLabels,
                                   @JsonProperty("matchExpressions") List<MatchExpression> matchExpressions) {
        return compile(Stream.concat(
                matchExpressions != null ? matchExpressions.stream() : Stream.empty(),
                (matchLabels != null ? matchLabels.entrySet().stream() : Stream.<Map.Entry<String, String>> empty())
                        .map(entry -> new MatchExpression(entry.getKey(), MatchOperator.IN, Set.of(entry.getValue()))))
                .toList());
    }

    /**
     * Convert this selector to a string respesentation.
     * This is the inverse operation to {@link #parse(String)}.
     * @return This string representation of this selector.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (MatchExpression e : matchExpressions) {
            if (!first) {
                sb.append(',');
            }
            sb.append(e.toString());
            first = false;
        }
        return sb.toString();
    }

    private Map<String, MatchExpression> byKey() {
        return matchExpressions.stream().collect(Collectors.toMap(MatchExpression::key, Function.identity()));
    }

    /**
     * Return true iff this selector is disjoint from the given {@code other} selector.
     * Two selectors are disjoint if the set of labels selected by each are disjoint (for all possible labels).
     * Disjointness is an irreflexive, symmetric, binary relation on selectors.
     * @param other The other selector
     * @return true if (and only if) this selector is disjoint from the other selector.
     */
    public boolean disjoint(Selector other) {
        // e.g. "foo,bar" is disjoint from "!foo,!bar"
        // "foo" is disjoint from "!foo,quux"
        // "foo,bar" is disjoint from "!bar,quux"
        // "foo in (a,b),bar" " is disjoint from "foo in (x),!bar"
        // one selector's keys must be a superset of the others, and the expressions for the intersecting keys must be disjoint
        Selector sup;
        Selector sub;
        if (MatchExpression.isSubset(this.byKey().keySet(), other.byKey().keySet())) {
            sub = this;
            sup = other;
        }
        else if (MatchExpression.isSubset(other.byKey().keySet(), this.byKey().keySet())) {
            sup = this;
            sub = other;
        }
        else {
            return false;
        }
        var intersection = intersection(sup.byKey().keySet(), sub.byKey().keySet());
        for (var key : intersection) {
            if (!sub.byKey().get(key).disjoint(sup.byKey().get(key))) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param other Another selector.
     * @return Return a selector that selects the labels selected by both this selector and the given {@code other} selector.
     */
    public Selector and(Selector other) {
        return compile(Stream.concat(this.matchExpressions.stream(), other.matchExpressions.stream()).toList());
    }

    /**
     * @param labels The labels to test against
     * @return true if and only if this selector matches the given labels.
     */
    @Override
    public boolean test(Map<String, String> labels) {
        for (var expr : matchExpressions()) {
            if (!expr.test(labels)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Parse the given string as a selector.
     * This is the inverse operation of {@link #toString()}.
     * @param selector The string representation of the selector
     * @return The selector
     * @throws LabelException if the {@code selector} string cannot be parsed as a selector.
     */
    public static Selector parse(String selector) {
        var parser = parserFor(selector);
        var selectorContext = parser.selector();
        ParseTreeWalker walker = new ParseTreeWalker();
        List<MatchExpression> matchExpressions = new ArrayList<>();
        walker.walk(new MatchExpressionExtractor(matchExpressions), selectorContext);
        return compile(matchExpressions);
    }

    @NonNull
    static SelectorParser parserFor(String selector) {
        var input = CharStreams.fromString(Objects.requireNonNull(selector));
        var lexer = new SelectorLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        var parser = new SelectorParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
                throw new LabelException("invalid selector syntax at " + line + ":" + charPositionInLine + ": " + msg);
            }
        });
        return parser;
    }

    /**
     * @return The expressions in this Selector
     */
    public List<MatchExpression> matchExpressions() {
        return matchExpressions;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (Selector) obj;
        return Objects.equals(this.matchExpressions, that.matchExpressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(matchExpressions);
    }

    private static class MatchExpressionExtractor extends SelectorBaseListener {

        private final List<MatchExpression> matchExpressions;
        private MatchExpression requirement;
        private Set<String> values;
        private String key;

        MatchExpressionExtractor(List<MatchExpression> matchExpressions) {
            this.matchExpressions = matchExpressions;
        }

        @Override
        public void visitErrorNode(ErrorNode node) {
            throw new LabelException("Error on " + node.getText() + " at " + node.getSymbol().getLine() + ":" + node.getSymbol().getCharPositionInLine());
        }

        @Override
        public void enterLabelName(SelectorParser.LabelNameContext ctx) {
            Labels.validateLabelName(ctx.getText());
        }

        @Override
        public void enterDnsSubdomain(SelectorParser.DnsSubdomainContext ctx) {
            Labels.validateLabelPrefix(ctx.getText());
        }

        @Override
        public void exitExistsRequirement(SelectorParser.ExistsRequirementContext ctx) {
            var operator = ctx.children.size() == 2 ? MatchOperator.NOT_EXISTS : MatchOperator.EXISTS;
            requirement = new MatchExpression(key, operator, null);
        }

        @Override
        public void exitRequirement(SelectorParser.RequirementContext ctx) {
            matchExpressions.add(Objects.requireNonNull(requirement));
            requirement = null;
        }

        @Override
        public void exitLabel(SelectorParser.LabelContext ctx) {
            key = ctx.getText();
        }

        @Override
        public void enterInRequirement(SelectorParser.InRequirementContext ctx) {
            values = new HashSet<>();
        }

        @Override
        public void exitInRequirement(SelectorParser.InRequirementContext ctx) {
            String text = ctx.children.get(1).getText();
            MatchOperator operator;
            if (text.equals("in")) {
                operator = MatchOperator.IN;
            }
            else {
                operator = MatchOperator.NOT_IN;
            }
            requirement = new MatchExpression(key, operator, values);
            key = null;
            values = null;
        }

        @Override
        public void enterValue(SelectorParser.ValueContext ctx) {
            Labels.validateLabelValue(ctx.getText());
        }

        @Override
        public void exitValue(SelectorParser.ValueContext ctx) {
            values.add(ctx.getText());
        }

        @Override
        public void enterEqualityRequirement(SelectorParser.EqualityRequirementContext ctx) {
            values = new HashSet<>(1);
        }

        @Override
        public void exitEqualityRequirement(SelectorParser.EqualityRequirementContext ctx) {
            String text = ctx.children.get(1).getText();
            MatchOperator operator;
            if (text.startsWith("=")) {
                operator = MatchOperator.IN;
            }
            else {
                operator = MatchOperator.NOT_IN;
            }
            if (values.size() != 1) {
                throw new IllegalStateException();
            }
            requirement = new MatchExpression(key, operator, values);
            key = null;
            values = null;
        }
    }

}
