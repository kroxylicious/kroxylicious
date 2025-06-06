/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.doxylicious.junit5;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotation for specifying a procedure to be tested.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@TestTemplate
@ExtendWith(ProcedureTestTemplateExtension.class)
public @interface TestProcedure {
    /**
     * @return The id of the procedure
     */
    String value();

    /**
     * @return The ids of prerequisites which should not be executed prior to the test
     */
    String[] assuming() default {};

    Fix[] fixing() default {};

    String workingDir() default ".";

    String timeout() default "30s";

    String destroyTimeout() default "10s";

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @interface Fix {
        String notional();

        String concrete();
    }
}
