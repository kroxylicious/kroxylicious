/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tag;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to document the thread on which the annotated code will be executed.
 */
@Documented
@Target({ ElementType.METHOD,
        ElementType.CONSTRUCTOR })
@Retention(RetentionPolicy.SOURCE)
public @interface RunsOnThread {
    /**
     * A description of the thread on which the annotated code is executed
     * @return
     */
    String value();
}
