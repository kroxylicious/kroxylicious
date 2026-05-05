/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotates plugin interface APIs with their "API version", like {@code v1beta1} or {@code v1}.
 * API versions must match the regular expression {@code v\\d+((alpha|beta)\\d+)?}.</p>
 *
 * <h2>Stability and compatibility</h2>
 * <p>Versions containing {@code alpha} may change incompatibly, or be removed entirely, even between Kroxylicious versions with the same major version number.
 * Versions containing {@code beta} may change incompatibly even between Kroxylicious versions with the same major version number.
 * Versions containing {@code alpha} or {@code beta} are collectively termed <em>unstable</em>.
 * <em>Stable</em> versions may evolve between Kroxylicious versions so long as they maintain backwards compatibility.
 * </p>
 *
 * <h2>Comparing versions</h2>
 * The semantics imply that API versions can be compared according to the API maturity.
 * For example: {@code v1alpha1 < v1alpha2 < v1beta1 < v1beta2 < v1 < v2alpha1< v2}.
 * This is different from the lexicographic ordering of the version strings themselves.
 */
@Documented
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface ApiVersion {
    String value();
}
