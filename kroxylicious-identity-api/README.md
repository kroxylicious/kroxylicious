# kroxylicious-identity-api Module

This module defines general-purpose identity types (`Subject`, `Principal`) in the `io.kroxylicious.identity` package.
See also [`../README.md`](../README.md) for project-wide context.

## Purpose

`Subject` and `Principal` are general authentication concepts, not proxy-specific ones.
This module has **zero runtime dependencies**, so any project — inside or outside Kroxylicious — can implement or consume APIs expressed in terms of these types (most notably [`kroxylicious-authorizer-api`](../kroxylicious-authorizer-api/README.md)) without pulling in `kafka-clients`, `jackson-annotations` or compression codec libraries.

This module exists because `io.kroxylicious.proxy.authentication.{Subject,Principal}` (in `kroxylicious-api`) previously served this purpose but forced consumers to depend on the whole of `kroxylicious-api`.
See [design proposal 119](https://github.com/kroxylicious/design/blob/main/proposals/119-auth-api-refactor.md) for the full rationale and migration plan.

## Types

**`Principal`** — an identifier held by a `Subject`:

```java
public interface Principal {
    String name();
}
```

Implementations must override `hashCode()`/`equals()` so that instances are equal if, and only if, they have the same implementation class and the same `name()`.
A `record` with a single `name` component gets this for free.

**`Subject`** — an actor in the system, composed of a possibly-empty set of principals:

```java
public record Subject(Set<? extends Principal> principals) implements Identity {
    public static Subject anonymous();
}
```

An anonymous subject has no principals; `Subject.anonymous()` returns the canonical instance.

**`@SingularPrincipal`** — annotates `Principal` implementations that are only intended to have a single instance in a `Subject`:

```java
@SingularPrincipal
public record Role(String name) implements Principal { }
```

Annotated types can then be used with `Subject.uniquePrincipalOfType(Class)` (inherited from `Identity`).
Constructing a `Subject` with more than one principal of a singular type throws `IllegalArgumentException`.

**`Identity`** — a deprecated-at-birth bridge interface, implemented by both this module's `Subject` and the deprecated `io.kroxylicious.proxy.authentication.Subject`:

```java
/** @deprecated Use {@link Subject} directly. Will be removed at 1.0. */
public interface Identity {
    Set<? extends Principal> principals();
    <P extends Principal> Optional<P> uniquePrincipalOfType(Class<P> type);
    <P extends Principal> Set<P> allPrincipalsOfType(Class<P> type);
    boolean isAnonymous();
}
```

`Identity` exists solely to let APIs like `Authorizer.authorize()` accept either the existing proxy `Subject` or this module's `Subject` during the migration described below.
Prefer `Subject` directly; do not depend on `Identity` in new code.

## Relationship to `kroxylicious-api`

`io.kroxylicious.proxy.authentication.{Subject,Principal,Unique}` in `kroxylicious-api` are deprecated in favour of the types in this module, but remain fully functional.
`FilterContext.authenticatedSubject()`, `RouterContext.authenticatedSubject()` and the transport/SASL subject builders continue to return/accept the existing `Subject` type until Kroxylicious 1.0 — no source changes are required in filter or router plugins today.
At 1.0, the bridge types (`Identity`, `SingularPrincipals`) will be removed and those APIs will migrate to `io.kroxylicious.identity.Subject`.

## Usage Notes for Consumers

- Prefer `Subject`/`Principal`/`@SingularPrincipal` from this module for any new, non-proxy-specific code.
- If you receive an `Identity` from an API such as `Authorizer.authorize()`, treat it as opaque — call `principals()`, `uniquePrincipalOfType()`, `allPrincipalsOfType()` or `isAnonymous()` on it; don't branch on its concrete type.
- This module has no dependency on Kafka, Jackson or compression libraries, and is expected to stay that way — see [java-dependency-changes.md](../.claude/rules/java-dependency-changes.md).

## Cross-References

- **Authorizer API**: See [`../kroxylicious-authorizer-api/README.md`](../kroxylicious-authorizer-api/README.md), the primary consumer of these types.
- **Filter API**: See [`../kroxylicious-api/README.md`](../kroxylicious-api/README.md) for the deprecated, proxy-specific `Subject`/`Principal` and how filters obtain an authenticated subject.
