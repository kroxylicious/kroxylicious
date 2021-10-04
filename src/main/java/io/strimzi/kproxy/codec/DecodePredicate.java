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
package io.strimzi.kproxy.codec;

import org.apache.kafka.common.protocol.ApiKeys;

/**
 * Determines whether a frame should be fully decoded into a {@link DecodedRequestFrame}
 * or {@link DecodedResponseFrame}, or whether it should be passed through un-encoded
 * as an {@link OpaqueFrame}.
 *
 * The determination of whether to decode either request or response is done when decoding a request.
 * The reason the decodability of a response is done when decoding a request is to avoid having to
 * record the request in the correlation map. i.e. The correlation map only contains entries for
 * decodable responses.
 */
public interface DecodePredicate {

    boolean shouldDecodeRequest(ApiKeys apiKey, int apiVersion);

    boolean shouldDecodeResponse(ApiKeys apiKey, int apiVersion);
}
