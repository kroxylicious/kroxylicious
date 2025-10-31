/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.acl;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.google.re2j.Pattern;

import io.kroxylicious.authorizer.service.Action;
import io.kroxylicious.authorizer.service.AuthorizeResult;
import io.kroxylicious.authorizer.service.Authorizer;
import io.kroxylicious.authorizer.service.Decision;
import io.kroxylicious.authorizer.service.ResourceType;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.authentication.Subject;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * <p>An implementation of {@link Authorizer} in terms of a list of rules defining <em>who</em> can do <em>what</em>
 * that is evaluated in-process.</p>
 *
 * <p>The rules are deny-by-default, meaning that subjects need to be <strong>explicitly granted</strong> permission to be able to perform any action.
 * There is no "super user", so <strong>every subject</strong> needs to be granted access to every resource.
 * However, to avoid needing a vast number of rules there are mechanisms to
 * grant access by subjects to resources <i>en-masse</i> using prefixes and regular expressions.
 * Also, it is possible to explicitly deny actions which are otherwise allowed by a "wider" pattern; this allows
 * granting access broadly while removing it in some specific cases, which is simpler than having to enumerate
 * every case where access should be allowed.
 * </p>
 *
 * <p>The set of rules can be defined:</p>
 * <ul>
 *     <li>programmatically, using the fluent API exposed by {@link #builder()},</li>
 *     <li>or externally, from a file which expresses the rules using naturalish language,
 *     according to a simple grammar (see {@code src/main/antlr4/io/kroxylicious/authorizer/provider/acl/parser/AclRules.g4}).</li>
 * </ul>
 */
public class AclAuthorizer implements Authorizer {

    enum Pred {
        ANY(TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY),
        EQ(TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL),
        STARTS(TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH),
        MATCH(null);

        private final @Nullable TypeNameMap.Predicate predicate;

        Pred(@Nullable TypeNameMap.Predicate predicate) {
            this.predicate = predicate;
        }

        public @Nullable TypeNameMap.Predicate toNameMatchPredicate() {
            return predicate;
        }
    }

    record PrincipalGrants(
                           @Nullable TypeNameMap<ResourceType<?>, EnumSet<? extends ResourceType<?>>> nameMatches,
                           @Nullable TypePatternMatch patternMatch) {

    }

    TypeNameMap<Principal, PrincipalGrants> denyPerPrincipal = new TypeNameMap<>();

    TypeNameMap<Principal, PrincipalGrants> allowPerPrincipal = new TypeNameMap<>();

    Set<Class<? extends ResourceType<?>>> usedResourceTypes = new HashSet<>();

    static Builder builder() {
        return new Builder();
    }

    public static class PrincipalSelectorBuilder {
        private final Builder builder;
        private final Class<? extends Principal> principalClass;
        private final boolean allow;

        public PrincipalSelectorBuilder(Builder builder,
                                        boolean allow,
                                        Class<? extends Principal> principalClass) {
            this.builder = builder;
            this.allow = allow;
            this.principalClass = principalClass;
        }

        public OperationsBuilder withNameEqualTo(String principalName) {
            return new OperationsBuilder(builder, allow, principalClass, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, Set.of(principalName));
        }

        public OperationsBuilder withNameIn(Set<String> principalNames) {
            return new OperationsBuilder(builder, allow, principalClass, TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, principalNames);
        }

        public OperationsBuilder withNameStartingWith(String principalNamePrefix) {
            return new OperationsBuilder(builder, allow, principalClass, TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, Set.of(principalNamePrefix));
        }

        public OperationsBuilder withAnyName() {
            HashSet<String> set = new HashSet<>();
            set.add(null);
            return new OperationsBuilder(builder, allow, principalClass, TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY, set);
        }
    }

    public static class SubjectSelectorBuilder {

        private final Builder builder;

        private final boolean allow;

        private SubjectSelectorBuilder(Builder builder, boolean allow) {
            this.builder = builder;
            this.allow = allow;
        }

        public PrincipalSelectorBuilder subjectsHavingPrincipal(Class<? extends Principal> userPrincipalClass) {
            return new PrincipalSelectorBuilder(builder, allow,
                    userPrincipalClass);
        }
    }

    public static class ResourceBuilder<O extends Enum<O> & ResourceType<O>> {
        private final Builder builder;
        private final Class<? extends Principal> principalClass;
        private final TypeNameMap.Predicate principalPred;
        private final Set<String> principalNames;
        private final Class<O> operationsClass;
        private final Set<O> operations;
        private final boolean allow;

        public ResourceBuilder(Builder builder,
                               boolean allow,
                               Class<? extends Principal> principalClass,
                               TypeNameMap.Predicate principalPred,
                               Set<String> principalNames,
                               Class<O> operationsClass,
                               Set<O> operations) {
            this.builder = Objects.requireNonNull(builder);
            this.allow = allow;
            this.principalClass = Objects.requireNonNull(principalClass);
            this.principalPred = Objects.requireNonNull(principalPred);
            this.principalNames = Objects.requireNonNull(principalNames);
            this.operationsClass = Objects.requireNonNull(operationsClass);
            this.operations = Objects.requireNonNull(operations);
        }

        public Builder onResourceWithNameEqualTo(String resourceName) {
            for (var principalName : principalNames) {
                builder.aclAuthorizer.internalAllowOrDeny(allow,
                        principalClass,
                        principalPred,
                        principalName,
                        operationsClass,
                        Pred.EQ,
                        resourceName,
                        operations);
            }
            return builder;
        }

        public Builder onResourcesWithNameIn(Set<String> resourceNames) {
            for (var principalName : principalNames) {
                for (String resourceName : resourceNames) {
                    builder.aclAuthorizer.internalAllowOrDeny(allow,
                            principalClass,
                            principalPred,
                            principalName,
                            operationsClass,
                            Pred.EQ,
                            resourceName,
                            operations);
                }
            }
            return builder;
        }

        public Builder onResourcesWithNameStartingWith(String resourceNamePrefix) {
            for (var principalName : principalNames) {
                builder.aclAuthorizer.internalAllowOrDeny(allow,
                        principalClass,
                        principalPred,
                        principalName,
                        operationsClass,
                        Pred.STARTS,
                        resourceNamePrefix,
                        operations);
            }
            return builder;
        }

        public Builder onResourcesWithNameMatching(String resourceNameRegex) {
            for (var principalName : principalNames) {
                builder.aclAuthorizer.internalAllowOrDeny(allow,
                        principalClass,
                        principalPred,
                        principalName,
                        operationsClass,
                        Pred.MATCH,
                        resourceNameRegex,
                        operations);
            }
            return builder;
        }

        public Builder onAllResources() {
            for (var principalName : principalNames) {
                builder.aclAuthorizer.internalAllowOrDeny(allow,
                        principalClass,
                        principalPred,
                        principalName,
                        operationsClass,
                        Pred.ANY,
                        null,
                        operations);
            }
            return builder;
        }
    }

    public static class OperationsBuilder {

        private final Builder builder;
        private final boolean allow;
        private final Class<? extends Principal> principalClass;
        private final TypeNameMap.Predicate principalPred;
        private final Set<String> principalNames;

        private OperationsBuilder(Builder builder,
                                  boolean allow,
                                  Class<? extends Principal> principalClass,
                                  TypeNameMap.Predicate principalPred,
                                  Set<String> principalNames) {
            this.builder = builder;
            this.allow = allow;
            this.principalPred = Objects.requireNonNull(principalPred);
            this.principalClass = Objects.requireNonNull(principalClass);
            this.principalNames = principalNames;
        }

        <O extends Enum<O> & ResourceType<O>> ResourceBuilder<O> allOperations(Class<O> cls) {
            return new ResourceBuilder<>(builder,
                    allow,
                    principalClass,
                    principalPred,
                    principalNames,
                    cls,
                    EnumSet.allOf(cls));
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public <O extends Enum<O> & ResourceType<O>> ResourceBuilder<O> operations(Set<O> operations) {
            EnumSet<O> os = EnumSet.copyOf(operations);
            return new ResourceBuilder<>(builder,
                    allow,
                    principalClass,
                    principalPred,
                    principalNames,
                    (Class) operations.iterator().next().getClass(),
                    os);
        }

    }

    public static class Builder {
        AclAuthorizer aclAuthorizer = new AclAuthorizer();

        public SubjectSelectorBuilder allow() {
            return new SubjectSelectorBuilder(this, true);
        }

        public SubjectSelectorBuilder deny() {
            return new SubjectSelectorBuilder(this, false);
        }

        public AclAuthorizer build() {
            return aclAuthorizer;
        }
    }

    /**
     * grant(READ, WRITE) on org.example.MyResource with name=R to UserPrincipals (bob, sue)
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    <O extends Enum<O> & ResourceType<O>> void grant(Set<O> operations,
                                                     String resourceName,
                                                     Set<Principal> principals) {
        for (var p : principals) {
            internalAllow(p.getClass(),
                    TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, p.name(),
                    (Class) operations.iterator().next().getClass(),
                    Pred.EQ, resourceName,
                    operations);
        }
    }

    <O extends Enum<O> & ResourceType<O>> void grantToAllPrincipalsOfType(Set<O> operations,
                                                                          String resourceName,
                                                                          Class<? extends Principal> principalType) {

        internalAllow(principalType,
                TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY, null,
                (Class) operations.iterator().next().getClass(),
                Pred.EQ, resourceName,
                operations);

    }

    private <O extends Enum<O> & ResourceType<O>> void internalAllow(Class<? extends Principal> principalType,
                                                                     TypeNameMap.Predicate principalPredicate,
                                                                     @Nullable String principalName,
                                                                     Class<O> opType,
                                                                     Pred resourceNamePredicate,
                                                                     @Nullable String resourceName,
                                                                     Set<O> operations) {
        internalAllowOrDeny(allowPerPrincipal, principalType, principalPredicate, principalName, opType, resourceNamePredicate, resourceName, operations);
    }

    private <O extends Enum<O> & ResourceType<O>> void internalAllowOrDeny(boolean allow,
                                                                           Class<? extends Principal> principalType,
                                                                           TypeNameMap.Predicate principalPredicate,
                                                                           @Nullable String principalName,
                                                                           Class<O> opType,
                                                                           Pred resourceNamePredicate,
                                                                           @Nullable String resourceName,
                                                                           Set<O> operations) {
        usedResourceTypes.add(opType);
        internalAllowOrDeny(allow ? allowPerPrincipal : denyPerPrincipal, principalType, principalPredicate, principalName, opType, resourceNamePredicate, resourceName,
                operations);
    }

    @SuppressWarnings("rawtypes")
    private static <O extends Enum<O> & ResourceType<O>> void internalAllowOrDeny(
                                                                                  TypeNameMap<Principal, PrincipalGrants> allowPerPrincipal,
                                                                                  Class<? extends Principal> principalType,
                                                                                  TypeNameMap.Predicate principalPredicate,
                                                                                  @Nullable String principalName,
                                                                                  Class<O> opType,
                                                                                  Pred resourceNamePredicate,
                                                                                  @Nullable String resourceName,
                                                                                  Set<O> operations) {
        // TODO fix the prefix stuff so we don't bother adding redundant prefixes
        var es = EnumSet.copyOf(operations);
        for (var op : es) {
            es.addAll(op.implies());
        }
        PrincipalGrants compute = allowPerPrincipal.compute(principalType, principalName, principalPredicate,
                g -> {
                    if (g == null) {
                        return new PrincipalGrants(resourceNamePredicate == Pred.MATCH ? null : new TypeNameMap<>(),
                                resourceNamePredicate == Pred.MATCH ? new TypePatternMatch() : null);
                    }
                    else if (g.patternMatch() == null && resourceNamePredicate == Pred.MATCH) {
                        return new PrincipalGrants(g.nameMatches(), new TypePatternMatch());
                    }
                    else if (g.nameMatches() == null && resourceNamePredicate != Pred.MATCH) {
                        return new PrincipalGrants(new TypeNameMap<>(), g.patternMatch());
                    }
                    return g;
                });

        if (resourceNamePredicate == Pred.MATCH) {
            Objects.requireNonNull(compute.patternMatch()).compute(opType, Pattern.compile(Objects.requireNonNull(resourceName)), operations);
        }
        else {
            Objects.requireNonNull(compute.nameMatches()).compute(opType, resourceName, Objects.requireNonNull(resourceNamePredicate.toNameMatchPredicate()),
                    v -> {
                        if (v == null) {
                            return es;
                        }
                        es.addAll((EnumSet) v);
                        return es;
                    });
        }
    }

    private static @Nullable Decision authorizeInternal(
                                                        TypeNameMap<Principal, PrincipalGrants> allowPerPrincipal,
                                                        Subject subject,
                                                        Action action,
                                                        Decision whenFound,
                                                        @Nullable Decision whenNotFound) {
        assert (whenFound != whenNotFound);
        for (var p : subject.principals()) {

            Decision foundDecision;
            var grant = allowPerPrincipal.lookup(p.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL, p.name());
            if (grant != null) {
                foundDecision = getDecision(action, grant, whenFound);
                if (foundDecision != null) {
                    return foundDecision;
                }
            }
            grant = allowPerPrincipal.lookup(p.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY, null);
            if (grant != null) {
                foundDecision = getDecision(action, grant, whenFound);
                if (foundDecision != null) {
                    return foundDecision;
                }
            }
            grant = allowPerPrincipal.lookup(p.getClass(), TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH, p.name());
            if (grant != null) {
                foundDecision = getDecision(action, grant, whenFound);
                if (foundDecision != null) {
                    return foundDecision;
                }
            }
        }
        return whenNotFound;
    }

    @Nullable
    private static Decision getDecision(Action action,
                                        PrincipalGrants grants,
                                        Decision whenFound) {
        Set<? extends ResourceType<?>> operations;
        var typeNameMap = grants.nameMatches();
        ResourceType<?> resourceType = action.operation();
        if (typeNameMap != null) {
            operations = typeNameMap.lookup(action.resourceTypeClass(),
                    TypeNameMap.Predicate.TYPE_EQUAL_NAME_EQUAL,
                    action.resourceName());
            if (isFound(operations, resourceType)) {
                return whenFound;
            }
            operations = typeNameMap.lookup(action.resourceTypeClass(),
                    TypeNameMap.Predicate.TYPE_EQUAL_NAME_ANY,
                    null);
            if (isFound(operations, resourceType)) {
                return whenFound;
            }
            operations = typeNameMap.lookup(action.resourceTypeClass(),
                    TypeNameMap.Predicate.TYPE_EQUAL_NAME_STARTS_WITH,
                    action.resourceName());
            if (isFound(operations, resourceType)) {
                return whenFound;
            }
        }
        var patternMatch = grants.patternMatch();
        if (patternMatch != null) {
            operations = patternMatch.lookup(action.resourceTypeClass(), action.resourceName());
            if (isFound(operations, resourceType)) {
                return whenFound;
            }
        }
        return null;
    }

    /**
     * Return true iff the given set of operations allow the given action.
     */
    private static boolean isFound(@Nullable Set<? extends ResourceType<?>> operations,
                                   ResourceType<?> op) {
        return operations != null && operations.contains(op);
    }

    @Override
    public CompletionStage<AuthorizeResult> authorize(Subject subject, List<Action> actions) {
        List<Action> allowedActions = new ArrayList<>();
        List<Action> deniedActions = new ArrayList<>();
        for (var action : actions) {
            @Nullable
            Decision decision = authorizeInternal(this.denyPerPrincipal, subject, action, Decision.DENY, null);
            if (decision == Decision.DENY) {
                deniedActions.add(action);
            }
            else {
                decision = authorizeInternal(this.allowPerPrincipal, subject, action, Decision.ALLOW, Decision.DENY);
                if (decision == Decision.DENY) {
                    deniedActions.add(action);
                }
                else if (decision == Decision.ALLOW) {
                    allowedActions.add(action);
                }
            }
            // TODO log it
        }
        return CompletableFuture.completedStage(new AuthorizeResult(subject, allowedActions, deniedActions));
    }

    @Override
    public Optional<Set<Class<? extends ResourceType<?>>>> supportedResourceTypes() {
        return Optional.of(Set.copyOf(usedResourceTypes));
    }

    @Override
    public String toString() {
        return "AclAuthorizer{" +
                "perPrincipal=" + allowPerPrincipal +
                '}';
    }
}
