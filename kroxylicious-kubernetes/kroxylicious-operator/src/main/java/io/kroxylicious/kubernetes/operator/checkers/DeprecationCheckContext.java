/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.checkers;

import java.util.ArrayList;

import org.slf4j.Logger;

import io.fabric8.kubernetes.client.CustomResource;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.operator.StatusFactory;

/**
 * Carries the inputs and outputs for a single deprecation check pass during reconciliation.
 *
 * <p>Checkers read from {@link #resource()} and {@link #statusFactory()} to construct
 * {@link Condition} instances, and append those conditions to {@link #conditions()}.
 * The {@link #logger()} allows checkers to emit structured log messages without needing
 * their own logger field.
 *
 * @param resource      the custom resource being reconciled
 * @param logger        the reconciler's logger, for use by checkers
 * @param statusFactory factory for building {@link Condition} instances
 * @param conditions    mutable list that checkers append deprecation conditions to
 *
 * @param <S> the spec type of the custom resource
 * @param <T> the status type of the custom resource
 * @param <R> the custom resource type
 * @param <F> the status factory type
 *
 * @see DeprecationChecker
 */
public record DeprecationCheckContext<S, T, R extends CustomResource<S, T>, F extends StatusFactory<R>>(
                                                                                                        R resource,
                                                                                                        Logger logger,
                                                                                                        F statusFactory,
                                                                                                        ArrayList<Condition> conditions) {}
