/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import edu.umd.cs.findbugs.annotations.NonNull;

import static java.util.function.Function.identity;

/**
 * The topic name's router exists to route a topic name to a destination
 * @param <D>
 */
public class TopicNameRouter<D> {

    private TopicNameRouter(List<Route<D>> routes) {
        Comparator<Route<D>> comparing = Comparator.comparingInt(Route::priority);
        this.routes = routes.stream().sorted(comparing.reversed()).collect(Collectors.toList());
    }

    private interface Route<D> {

        boolean test(String topicName);

        int priority();

        D destination();
    }

    record MatchAny<D>(D destination) implements Route<D> {

        MatchAny {
            Objects.requireNonNull(destination);
        }

        @Override
        public boolean test(String topicName) {
            return true;
        }

        @Override
        public int priority() {
            return 0;
        }
    }

    record MatchExact<D>(String topicName, D destination) implements Route<D> {

        MatchExact {
            Objects.requireNonNull(topicName);
            Objects.requireNonNull(destination);
        }

        @Override
        public boolean test(String topicName) {
            return topicName.equals(this.topicName);
        }

        @Override
        public int priority() {
            return Integer.MAX_VALUE;
        }
    }

    record MatchPrefix<D>(String topicNamePrefix, D destination) implements Route<D> {

        MatchPrefix {
            Objects.requireNonNull(topicNamePrefix);
            Objects.requireNonNull(destination);
        }

        @Override
        public boolean test(String topicName) {
            return topicName.startsWith(topicNamePrefix);
        }

        @Override
        public int priority() {
            return topicNamePrefix.length();
        }
    }

    private final List<Route<D>> routes;

    Map<D, Set<String>> route(Set<String> topicName) {
        Map<String, D> nameToRoute = topicName.stream().collect(Collectors.toMap(identity(), this::route));
        return invert(nameToRoute);
    }

    static <A, B> Map<A, Set<B>> invert(Map<B, A> map) {
        HashMap<A, Set<B>> inverted = new HashMap<>();
        for (Map.Entry<B, A> entry : map.entrySet()) {
            Set<B> bs = inverted.computeIfAbsent(entry.getValue(), a -> new HashSet<>());
            bs.add(entry.getKey());
        }
        return inverted;
    }

    @NonNull
    private D route(String s) {
        return routes.stream().filter(dRoute -> dRoute.test(s)).map(Route::destination).findFirst().orElseThrow();
    }

    static <D> Builder<D> builder(D defaultDestination) {
        return new Builder<>(defaultDestination);
    }

    static class Builder<D> {
        private final List<Route<D>> include = new ArrayList<>();
        private final List<Route<D>> exclude = new ArrayList<>();
        private final D defaultDestination;

        Builder(D defaultDestination) {
            Objects.requireNonNull(defaultDestination);
            this.defaultDestination = defaultDestination;
            this.include.add(new MatchAny<>(defaultDestination));
        }

        TopicNameRouter<D> build() {
            ArrayList<Route<D>> allRoutes = new ArrayList<>(include);
            allRoutes.addAll(exclude);
            return new TopicNameRouter<>(allRoutes);
        }

        public Builder<D> addIncludeExactNameRoute(String topicName, D destination) {
            removeExclustionsMatchedExactly(topicName);
            if (containsExactName(topicName, include)) {
                throw new IllegalStateException(topicName + " already has an inclusion route ");
            }
            this.include.add(new MatchExact<>(topicName, destination));
            return this;
        }

        private boolean containsExactName(String topicName, List<Route<D>> routes) {
            return routes.stream().anyMatch(dRoute -> dRoute instanceof TopicNameRouter.MatchExact<D> m && m.topicName.equals(topicName));
        }

        private boolean containsPrefix(String prefix, List<Route<D>> routes) {
            return routes.stream().anyMatch(dRoute -> dRoute instanceof TopicNameRouter.MatchPrefix<D> m && m.topicNamePrefix.equals(prefix));
        }

        public Builder<D> addIncludePrefixRoute(String prefix, D destination) {
            if (containsPrefix(prefix, include)) {
                throw new IllegalStateException(prefix + " prefix already has an inclusion route ");
            }
            removeExclusionsMatchedByPrefix(prefix);
            this.include.add(new MatchPrefix<>(prefix, destination));
            return this;
        }

        private void removeExclustionsMatchedExactly(String topicName) {
            this.exclude.removeIf(excludedRoute -> excludedRoute instanceof TopicNameRouter.MatchExact<D> matchExact && matchExact.topicName.equals(topicName));
        }

        private void removeExclusionsMatchedByPrefix(String prefix) {
            this.exclude.removeIf(excludedRoute -> excludedRoute instanceof TopicNameRouter.MatchPrefix<D> matchPrefix && matchPrefix.topicNamePrefix.startsWith(prefix));
            this.exclude.removeIf(excludedRoute -> excludedRoute instanceof TopicNameRouter.MatchExact<D> matchPrefix && matchPrefix.topicName.startsWith(prefix));
        }

        public Builder<D> addExcludePrefixRoute(String prefix) {
            if (!containsPrefix(prefix, include)) {
                this.exclude.add(new MatchPrefix<>(prefix, defaultDestination));
            }
            return this;
        }

        public Builder<D> addExcludeExactNameRoute(String topicName) {
            if (!containsExactName(topicName, include)) {
                this.exclude.add(new MatchExact<>(topicName, defaultDestination));
            }
            return this;
        }
    }

}
