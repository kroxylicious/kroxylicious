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
 package io.strimzi.kproxy.api.filter;

 import org.apache.kafka.common.message.UpdateFeaturesRequestData;

/**
 * A stateless filter for UpdateFeaturesRequests.
 * The same instance may be invoked on multiple channels.
 */
public interface UpdateFeaturesRequestFilter {

    /**
     * Handle the given {@code data},
     * returning the {@code UpdateFeaturesRequestData} instance to be passed to the next filter.
     * The implementation may modify the given {@code data} in-place and return it,
     * or instantiate a new one.
     *
     * @param data The KRPC message to handle.
     * @param context The context.
     * @return the {@code UpdateFeaturesRequestData} instance to be passed to the next filter.
     * If null is returned then the given {code data} will be used.
     */
    public UpdateFeaturesRequestData onUpdateFeaturesRequest(UpdateFeaturesRequestData data, FilterContext context);
}