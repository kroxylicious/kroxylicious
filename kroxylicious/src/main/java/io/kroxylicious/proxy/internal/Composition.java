/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kroxylicious.proxy.internal;

import java.util.function.Function;

import io.kroxylicious.proxy.future.ProxyFuture;
import io.kroxylicious.proxy.future.ProxyPromise;

class Composition<T, U> implements ProxyPromiseImpl.Listener<T> {

    private final Function<T, ProxyFuture<U>> successMapper;
    private final Function<Throwable, ProxyFuture<U>> failureMapper;

    Composition(Function<T, ProxyFuture<U>> successMapper, Function<Throwable, ProxyFuture<U>> failureMapper) {
        this.successMapper = successMapper;
        this.failureMapper = failureMapper;
    }

    @Override
    public void onSuccess(T value) {
        ProxyPromiseImpl<U> future;
        try {
            future = (ProxyPromiseImpl<U>) successMapper.apply(value);
        } catch (Throwable e) {
            tryFail(e);
            return;
        }
        future.addListener(newListener());
    }

    @Override
    public void onFailure(Throwable failure) {
        ProxyPromiseImpl<U> future;
        try {
            future = (ProxyPromiseImpl<U>) failureMapper.apply(failure);
        } catch (Throwable e) {
            tryFail(e);
            return;
        }
        future.addListener(newListener());
    }

    private ProxyPromiseImpl.Listener<U> newListener() {
        return new ProxyPromiseImpl.Listener<U>() {
            @Override
            public void onSuccess(U value) {
                tryComplete(value);
            }
            @Override
            public void onFailure(Throwable failure) {
                tryFail(failure);
            }
        };
    }
}
