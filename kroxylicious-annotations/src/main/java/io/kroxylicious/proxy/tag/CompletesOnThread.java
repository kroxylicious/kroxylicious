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
import java.util.function.Function;

/**
 * Annotation to document the thread on which an
 * asynchronous result callback, such as might be passed to
 * {@link java.util.concurrent.CompletionStage#thenApply(Function)}, will be executed.
 * The intention is that this annotation is applied to methods which return CompletionStage,
 * parameters of type CompletionStage, and so on.
 */
@Documented
@Target({ ElementType.TYPE_USE, ElementType.PARAMETER,
        ElementType.METHOD,
        ElementType.FIELD,
        ElementType.LOCAL_VARIABLE,
        ElementType.RECORD_COMPONENT })
@Retention(RetentionPolicy.SOURCE)
public @interface CompletesOnThread {
    /**
     * A description of the thread on which the {@link java.util.concurrent.CompletableFuture},
     * {@link java.util.concurrent.CompletionStage} or other asynchronous result
     * is completed. Use "*" if the asynchronous result can complete on any thread
     * (for example on usage in an interface which doesn't define a policy for implementors)
     * @return
     */
    String value();
}
