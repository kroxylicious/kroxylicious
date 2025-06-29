/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Result for a validation
 * @param valid whether the input was valid
 * @param errorMessage error message that should be supplied when input is invalid
 */
public record Result(boolean valid, @Nullable String errorMessage) {

    public static final Result VALID_RESULT = new Result(true, null);
    public static CompletionStage<Result> VALID_RESULT_STAGE = CompletableFuture.completedFuture(VALID_RESULT);
}
