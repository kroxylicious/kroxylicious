/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters;

import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;

import io.kroxylicious.proxy.filter.CreateTopicsRequestFilter;
import io.kroxylicious.proxy.filter.CreateTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsRequestFilter;
import io.kroxylicious.proxy.filter.DeleteTopicsResponseFilter;
import io.kroxylicious.proxy.filter.DescribeGroupsRequestFilter;
import io.kroxylicious.proxy.filter.DescribeGroupsResponseFilter;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;
import io.kroxylicious.proxy.filter.RequestFilterCommand;
import io.kroxylicious.proxy.filter.ResponseFilterCommand;

public class EightInterfaceFilter implements ProduceResponseFilter, ProduceRequestFilter, CreateTopicsRequestFilter, CreateTopicsResponseFilter,
        DeleteTopicsRequestFilter, DeleteTopicsResponseFilter, DescribeGroupsRequestFilter, DescribeGroupsResponseFilter {

    @Override
    public CompletionStage<RequestFilterCommand> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        return null;
    }

    @Override
    public CompletionStage<ResponseFilterCommand> onProduceResponse(short apiVersion, ResponseHeaderData header, ProduceResponseData response,
                                                                    FilterContext context) {
        return null;
    }

    @Override
    public CompletionStage<RequestFilterCommand> onCreateTopicsRequest(short apiVersion, RequestHeaderData header, CreateTopicsRequestData request,
                                                                       FilterContext context) {

        return null;
    }

    @Override
    public CompletionStage<ResponseFilterCommand> onCreateTopicsResponse(short apiVersion, ResponseHeaderData header, CreateTopicsResponseData response,
                                                                         FilterContext context) {

        return null;
    }

    @Override
    public CompletionStage<RequestFilterCommand> onDeleteTopicsRequest(short apiVersion, RequestHeaderData header, DeleteTopicsRequestData request,
                                                                       FilterContext context) {

        return null;
    }

    @Override
    public CompletionStage<ResponseFilterCommand> onDeleteTopicsResponse(short apiVersion, ResponseHeaderData header, DeleteTopicsResponseData response,
                                                                         FilterContext context) {

        return null;
    }

    @Override
    public CompletionStage<RequestFilterCommand> onDescribeGroupsRequest(short apiVersion, RequestHeaderData header, DescribeGroupsRequestData request,
                                                                         FilterContext context) {

        return null;
    }

    @Override
    public CompletionStage<ResponseFilterCommand> onDescribeGroupsResponse(short apiVersion, ResponseHeaderData header, DescribeGroupsResponseData response,
                                                                           FilterContext context) {

        return null;
    }
}
