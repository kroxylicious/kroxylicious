/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.identity;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * <p>Shared validation utility for {@linkplain SingularPrincipal singular} principal types.
 * Both {@link Subject} and the deprecated {@code io.kroxylicious.proxy.authentication.Subject}
 * delegate their constructor uniqueness checks here.</p>
 *
 * <p>{@link #isSingular(Class)} uses a one-level meta-annotation scan so that types annotated
 * with an annotation that is itself meta-annotated with {@link SingularPrincipal} (such as the
 * deprecated {@code io.kroxylicious.proxy.authentication.Unique}) are recognised without this
 * module depending on that annotation.</p>
 *
 * @deprecated Transitional utility. At 1.0 the meta-annotation scanning will no longer be needed
 * and this validation should be inlined into {@link Subject} directly.
 */
@Deprecated(since = "0.24.0", forRemoval = true)
public final class SingularPrincipals {

    private SingularPrincipals() {
    }

    /**
     * Returns whether the given type should have at most one instance in a {@link Subject}.
     * A type is singular if it is annotated with {@link SingularPrincipal} directly, or if any
     * annotation present on the type is itself meta-annotated with {@link SingularPrincipal}
     * (a one-level scan).
     * @param type the principal type to test
     * @return true if the type is a singular principal type
     */
    public static boolean isSingular(Class<?> type) {
        if (type.isAnnotationPresent(SingularPrincipal.class)) {
            return true;
        }
        for (Annotation annotation : type.getAnnotations()) {
            if (annotation.annotationType().isAnnotationPresent(SingularPrincipal.class)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Validates that at most one principal of each singular type is present.
     * @param principals the principals to validate
     * @throws IllegalArgumentException if any singular principal type has more than one instance
     */
    public static void validateUniqueness(Set<? extends Principal> principals) {
        Map<Class<?>, Integer> countByType = new HashMap<>();
        for (Principal principal : principals) {
            countByType.merge(principal.getClass(), 1, Integer::sum);
        }
        countByType.forEach((principalClass, count) -> {
            if (count > 1 && isSingular(principalClass)) {
                throw new IllegalArgumentException(
                        count + " principals of " + principalClass + " were found, but " + principalClass
                                + " is a singular principal type.");
            }
        });
    }
}
