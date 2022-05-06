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
package io.strimzi.kproxy.interceptor;

import org.apache.kafka.common.protocol.ApiKeys;

import io.strimzi.kproxy.codec.DecodePredicate;

/**
 * An interceptor of Kafka messages.
 * <p>
 * The interceptor must declare which requests and responses it wants to decode using
 * {@link DecodePredicate#shouldDecodeRequest(ApiKeys, int)} and {@link DecodePredicate#shouldDecodeResponse(ApiKeys, int)}.
 * It should also provide a non-null {@link #requestHandler()} for those requests where
 * {@link DecodePredicate#shouldDecodeRequest(ApiKeys, int)} returns true,
 * and a non-null {@link #responseHandler()} for those responses where
 * {@link DecodePredicate#shouldDecodeResponse(ApiKeys, int) returns true.
 */
@Deprecated
public interface Interceptor extends DecodePredicate {

    RequestHandler requestHandler();

    ResponseHandler responseHandler();
}
