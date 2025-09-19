/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.authorization;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import edu.umd.cs.findbugs.annotations.Nullable;

public class SimpleAuthorizer implements Authorizer {
/*
    record ResourceKey(
            Class<? extends Operation<?>> resourceType,
            @Nullable String resourceName
    ) implements Comparable<ResourceKey> {
        @Override
        public int compareTo(ResourceKey o) {
            var cmp = this.resourceType.getName().compareTo(o.resourceType.getName());
            if (cmp == 0) {
                if (this.resourceName == null) {
                    cmp = o.resourceName == null ? 0 : -1;
                }
                else if (o.resourceName == null) {
                    cmp = 1;
                }
                else {
                    cmp = this.resourceName.compareTo(o.resourceName);
                }
            }
            return cmp;
        }
    }

    private static ResourceKey resourceKey(Class<? extends Operation<?>> resourceType,
                                           @Nullable String resourceName) {
        return new ResourceKey(resourceType, resourceName);
    }
*/
    record ClassNameKey<T>(
            Class<T> type,
            @Nullable String name
    ) implements Comparable<ClassNameKey<T>> {

        static ClassNameKey<? extends Principal> forPrincipal(Principal p) {
            return new ClassNameKey<>(p.getClass(), p.name());
        }

        private static ClassNameKey<? extends Operation<?>> forResource(Class<? extends Operation<?>> resourceType,
                                               @Nullable String resourceName) {
            return new ClassNameKey<>(resourceType, resourceName);
        }

        ClassNameKey {
            Objects.requireNonNull(type);
        }

        @Override
        public int compareTo(ClassNameKey<T> o) {
            var cmp = this.type.getName().compareTo(o.type.getName());
            if (cmp == 0) {
                if (this.name == null) {
                    cmp = o.name == null ? 0 : -1;
                }
                else if (o.name == null) {
                    cmp = 1;
                }
                else {
                    cmp = this.name.compareTo(o.name);
                }
            }
            return cmp;
        }
    }

    Map<ClassNameKey<? extends Principal>, Map<ClassNameKey<? extends Operation<?>>, EnumSet<? extends Operation<?>>>> perPrincipal = new TreeMap<>();

    /*
    sealed interface Pattern<T> permits All, StartsWith, Equals {
        <V> V search(TreeMap<ClassNameKey<? extends Operation<?>>, V> map,
                              Class<? extends Operation<?>> resourceType, String value);
    }

    record All<T>(Class<T> resourceType) implements Pattern<T> {
        public <V> V search(TreeMap<ClassNameKey<? extends Operation<?>>, V> map,
                     Class<? extends Operation<?>> resourceType, String value) {
            return map.get(ClassNameKey.forResource(resourceType, null));
        }
    }

    record StartsWith<T>(Class<T> resourceType, String prefix) implements Pattern<T> {
        public <V> V search(TreeMap<ClassNameKey<? extends Operation<?>>, V> map,
                            Class<? extends Operation<?>> resourceType, String value) {
            // TODO it's not a get, it's a subtree
            ResourceKey resourceKey = new ResourceKey(resourceType, "<" + prefix);
            SortedMap<ResourceKey, V> resourceKeyVSortedMap = map.subMap(resourceKey, resourceKey);
            return resourceKeyVSortedMap;
        }
    }
    record Equals<T>(Class<T> resourceType, String value) implements Pattern<T> {
        public <V> V search(TreeMap<ResourceKey, V> map,
                            Class<? extends Operation<?>> resourceType, String value) {
            // TODO it's not a get, it's a subtree
            return map.get(new ResourceKey(resourceType, "=" + value)));
        }
    }*/

    // TODO allow a way to say "grant * on {T} with {any name} to subject with {com.example.UserPrincipal in (bob, sue)}"
    // TODO allow a way to say "grant * on {any resource} with {any name} to subject with {com.example.UserPrincipal in (bob, sue)}"
    // TODO allow a way to say "grant * on {any resource} with {any name} with to {any subject}"

    static Builder builder() {
        return new Builder();
    }

    public static class PrincipalSelectorBuilder {
        private final Builder builder;
        private final Class<? extends Principal> principalClass;
        private final Set<String> resourceNames;

        public PrincipalSelectorBuilder(Builder builder, Set<String> resourceNames, Class<? extends Principal> principalClass) {
            this.builder = builder;
            this.resourceNames = resourceNames;
            this.principalClass = principalClass;
        }

        public Builder withNameEqualTo(String s) {
            for (String resourceName : resourceNames) {
                // TODO here I need to instantiate the principal
                // Or should we just use a Pair<Class, String> as the identifier
                // Or should we just use a Pair<ClassName, String> as the identifier
                builder.simpleAuthorizer.internalGrant(null, null, resourceName, null);
            }
            return builder;
        }
    }

    public static class SubjectSelectorBuilder {

        private final Builder builder;
        private final Set<String> resourceNames;

        public SubjectSelectorBuilder(Builder builder) {
            this.builder = builder;
            this.resourceNames = null;
        }

        public SubjectSelectorBuilder(Builder builder, Set<String> resourceNames) {
            this.builder = builder;
            this.resourceNames = resourceNames;
        }

        public PrincipalSelectorBuilder toSubjectsHavingPrincipal(Class<? extends Principal> userPrincipalClass) {
            return new PrincipalSelectorBuilder(builder, resourceNames, userPrincipalClass);
        }
    }

    public static class ResourceBuilder {
        private final Builder builder;

        public ResourceBuilder(Builder builder) {
            this.builder = builder;
        }

        public SubjectSelectorBuilder forResourcesWithNameIn(Set<String> names) {
            return new SubjectSelectorBuilder(builder, names);
        }

        public SubjectSelectorBuilder forResourceWithNameEqualTo(String name) {
            return new SubjectSelectorBuilder(builder, Set.of(name));
        }

        public SubjectSelectorBuilder forResourcesWithNameStartingWith(String prefix) {
            return new SubjectSelectorBuilder(builder);
        }

        public SubjectSelectorBuilder forAllResources() {
            // the empty string is a prefix of every string
            return forResourcesWithNameStartingWith("");
        }
    }

    public static class GrantBuilder {

        private Builder builder;

        private GrantBuilder(Builder builder) {
            this.builder = builder;
        }

        <O extends Enum<O> & Operation<O>> ResourceBuilder allOperations(Class<O> cls) {
            return new ResourceBuilder(builder);
        }

        public <O extends Enum<O> & Operation<O>> ResourceBuilder operations(Set<O> operations) {
            return new ResourceBuilder(builder);
        }

    }

    public static class Builder {
        SimpleAuthorizer simpleAuthorizer = new SimpleAuthorizer();

        public GrantBuilder grant() {
            return new GrantBuilder(this);
        }

        public SimpleAuthorizer build() {
            return simpleAuthorizer;
        }
    }

    void foo() {
        SimpleAuthorizer.builder()
                .grant()
                    .allOperations(TopicResource.class)
                    .forResourceWithNameEqualTo("my-topic")
                    .toSubjectsHavingPrincipal(UserPrincipal.class)
                    .withNameEqualTo("bob")
                .grant()
                    .operations(Set.of(TopicResource.READ))
                    .forResourcesWithNameStartingWith("")
                    .toSubjectsHavingPrincipal(UserPrincipal.class)
                    .withNameEqualTo("alice")
                .build();
    }

    /**
     * grant * on org.example.MyResource with name=R to com.example.UserPrincipal (bob, sue)
     */
    public <O extends Enum<O> & Operation<O>> void grantAll(Class<O> opClass,
                                                            String resourceName,
                                                            Set<Principal> principals) {
        grant(EnumSet.allOf(opClass), resourceName, principals);
    }

    /**
     * grant(READ, WRITE) on org.example.MyResource with name=R to UserPrincipals (bob, sue)
     */
    public <O extends Enum<O> & Operation<O>> void grant(Set<O> operations,
                                                         String resourceName,
                                                         Set<Principal> principals) {
        internalGrant((Class) operations.iterator().next().getClass(), operations, resourceName,
                principals.stream().map(ClassNameKey::forPrincipal).toList());
    }

    public <O extends Enum<O> & Operation<O>> void grantToAllPrincipalsOfType(Set<O> operations,
                                                         String resourceName,
                                                         Class<? extends Principal> principalType) {
        internalGrant((Class) operations.iterator().next().getClass(), operations, resourceName,
                List.of(new ClassNameKey<>(principalType, null)));
    }

    private <O extends Enum<O> & Operation<O>> void internalGrant(Class<O> opType,
                                                                  Set<O> operations,
                                                                  String resourceName,
                                                                  Iterable<? extends ClassNameKey<? extends Principal>> principalKeys) {
        var es = EnumSet.copyOf(operations);
        for (var pk : principalKeys) {
            perPrincipal.computeIfAbsent(pk,
                    p1 -> new TreeMap<>()).compute(ClassNameKey.forResource(opType, resourceName),
                    (k, v) -> {
                        if (v == null) {
                            return es;
                        }
                        es.addAll((EnumSet) v);
                        return v;
                    });
        }
    }

    public Decision authorize(Subject subject, Action action) {
        for (var p : subject.principals()) {
            var grant = perPrincipal.get(ClassNameKey.forPrincipal(p));

            if (grant != null) {
                EnumSet<? extends Operation<?>> objects = grant.get(ClassNameKey.forResource(action.resourceType(), action.resourceName()));
                if (objects != null && objects.contains(action.operation())) {
                    return Decision.ALLOW;
                }
                objects = grant.get(ClassNameKey.forResource(action.resourceType(), null));
                if (objects != null && objects.contains(action.operation())) {
                    return Decision.ALLOW;
                }
            }

            grant = perPrincipal.get(new ClassNameKey<>(p.getClass(), null));

            if (grant != null) {
                EnumSet<? extends Operation<?>>  objects = grant.get(ClassNameKey.forResource(action.resourceType(), action.resourceName()));
                if (objects != null && objects.contains(action.operation())) {
                    return Decision.ALLOW;
                }
                objects = grant.get(ClassNameKey.forResource(action.resourceType(), null));
                if (objects != null && objects.contains(action.operation())) {
                    return Decision.ALLOW;
                }
            }
        }
        return Decision.DENY;
    }

    @Override
    public Authorization authorize(Subject subject, List<Action> actions) {
        List<Action> allowedActions = new ArrayList<>();
        List<Action> deniedActions = new ArrayList<>();
        for (var action : actions) {
            switch (authorize(subject, action)) {
                case DENY -> deniedActions.add(action);
                case ALLOW -> allowedActions.add(action);
            }
            // TODO log it
        }
        return new Authorization(allowedActions, deniedActions);
    }

    @Override
    public String toString() {
        return "SimpleAuthorizer{" +
                "perPrincipal=" + perPrincipal +
                '}';
    }
}

