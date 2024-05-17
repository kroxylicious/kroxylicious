/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metadata.handler;

class LabelIndex<V> {
    /*
     * record IndexKey(@NonNull String labelKey, @NonNull String labelValue) implements Comparable<IndexKey> {
     * IndexKey {
     * Objects.requireNonNull(labelKey);
     * Objects.requireNonNull(labelValue);
     * }
     *
     * @Override
     * public int compareTo(IndexKey o) {
     * int i = labelKey.compareTo(o.labelKey);
     * if (i == 0) {
     * i = labelKey.compareTo(o.labelValue);
     * }
     * return i;
     * }
     * }
     *
     * TreeMap<IndexKey, V> index;
     *
     * public void insert(String labelKey, String labelValue, V value) {
     * index.put(new IndexKey(labelKey, labelValue), value);
     * }
     *
     * interface Eval<V, R> {
     * R eval(NavigableMap<IndexKey, V> map);
     * }
     *
     * record Range<V>(IndexKey from, boolean fromInclusive, IndexKey to, boolean toInclusive) implements Eval<V, Set<V>> {
     * Range {
     * if (from.compareTo(to) > 0) {
     * throw new IllegalArgumentException();
     * }
     * }
     *
     * @Override
     * public Set<V> eval(NavigableMap<IndexKey, V> map) {
     * return Set.copyOf(map.subMap(from, fromInclusive, to, toInclusive).values());
     * }
     * }
     *
     * record Union<V, R>(List<? extends Eval<V, ? extends Collection<R>>> ranges) implements Eval<V, Set<R>> {
     *
     * @Override
     * public Set<R> eval(NavigableMap<IndexKey, V> map) {
     * Set<R> result = new HashSet<>();
     * for (var range : ranges) {
     * result.addAll(range.eval(map));
     * }
     * return result;
     * }
     * // TODO canonicalize the ranges
     * }
     *
     * record Intersection<V, R>(List<? extends Eval<V, ? extends Collection<R>>> ranges) implements Eval<V, Set<R>> {
     *
     * @Override
     * public Set<R> eval(NavigableMap<IndexKey, V> map) {
     * HashSet<R> result = new HashSet<>();
     * for (var range : ranges) {
     * result.retainAll(range.eval(map));
     * }
     * return result;
     * }
     * // TODO canonicalize the ranges
     * }
     *
     * record Pair<V>(Selector selector, Set<V> set) {}
     *
     * record KeyedEval<V>(Selector selector, Eval<V, Set<V>> eval) implements Eval<V, Pair<Set<V>>> {
     *
     * @Override
     * public Pair<Set<V>> eval(NavigableMap<IndexKey, V> map) {
     * return new Pair(selector, eval.eval(map));
     * }
     * }
     *
     * public Map<Selector, Set<V>> query(Collection<Selector> selectors) {
     * Eval<V> plan = plan(selectors);
     * plan.eval(index);
     * }
     *
     * private Eval<Pair<V>> plan(Collection<Selector> selectors) {
     * List<KeyedEval<V>> l = new ArrayList<>();
     * for (var selector : selectors) {
     * var eval = plan(selector);
     * l.add(new KeyedEval<>(selector, eval));
     * }
     * Union<V, Pair<Set<V>>> vrUnion = new Union<>(l);
     * return vrUnion;
     * }
     *
     * private Eval<V, Set<V>> plan(Selector selector) {
     * List<Eval<V, Set<V>>> list = selector.matchExpressions().stream().map(this::matchExpression).toList();
     * return new Intersection<>(list);
     * }
     *
     * // TODO convert each selector into a plan
     * // foo => Range([foo=, foo\0=)) add
     * Eval<V, Set<V>> exists(String key) {
     * return new Range<V>(
     * new IndexKey(key, ""), true,
     * new IndexKey(key + '0', ""), true);
     * }
     *
     * // foo in (x) => Range([foo=x, foo=x]) add
     * // foo in (x,y) => Composite(Range([foo=x, foo=x]) add, Range([foo=x, foo=x]) add)
     * Eval<V, Set<V>> in(String key, Set<String> values) {
     * List<Range<V>> subRanges = new ArrayList<>();
     * for (String value : values) {
     * subRanges.add(new Range<V>(
     * new IndexKey(key, value), true,
     * new IndexKey(key, value), true));
     * }
     * return new Union(subRanges);
     * }
     *
     * // !foo => Composite(Range([\0=\0, foo=)) add, Range((foo\0=,]) add)
     * Eval<V, Set<V>> notExists(String key) {
     * return new Union<V, Set<V>>(List.of(
     * new Range<V>(
     * new IndexKey("", ""), true,
     * new IndexKey(key, ""), false),
     * new Range<V>(
     * new IndexKey(key + '\0', ""), true,
     * new IndexKey(KEY_UPPER_BOUND, ""), false)));
     * }
     *
     * // foo notin (x) => Range(["", foo=x)) add
     * // union
     * // Range((foo=x, )) add
     * Eval<V, Set<V>> notIn(String key, Set<String> values) {
     * throw new UnsupportedOperationException();
     * }
     *
     * @NonNull
     * Eval<V, Set<V>> matchExpression(@NonNull MatchExpression matchExpression) {
     * return switch (matchExpression.operator()) {
     * case EXISTS -> exists(matchExpression.key());
     * case NOT_EXISTS -> notExists(matchExpression.key());
     * case IN -> in(matchExpression.key(), matchExpression.values());
     * case NOT_IN -> notIn(matchExpression.key(), matchExpression.values());
     * };
     * }
     *
     * // X,Y => X intersection Y => order the x_i and y_i by key, the first one -- min(x_0, y_0) -- instantiates a set
     * // for
     * // Composite is more like a Union, with a pre-step of creating a set, and an action of adding
     * // Intersection has a first-call action of creating a set and adding all the first results to it, an each subsequent is a retainAll
     *
     * // "" is <= all other strings
     * // "foo" <= all strings beginning with "foo"
     * // "foo\0" > "foo", but < all other strings beginning with "foo"
     * // since label keys and values are not straight unicode, there's an upper bound for each
     * // the lub non-legal unicode value for their first letter
     *
     * IndexKey lowerBound(String key, boolean inclusiveKey, String value, boolean inclusiveValue) {
     * return new IndexKey(
     * inclusiveKey ? key : key + '\0',
     * inclusiveValue ? value : value + '\0');
     * }
     *
     * IndexKey upperBound(String key, boolean inclusiveKey, String value, boolean inclusiveValue) {
     * return new IndexKey(
     * inclusiveKey ? key : key + '\0',
     * inclusiveValue ? value : value + '\0');
     * }
     */
}
