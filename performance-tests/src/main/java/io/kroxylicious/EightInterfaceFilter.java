/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

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
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.ProduceResponseFilter;

public class EightInterfaceFilter implements ProduceResponseFilter, ProduceRequestFilter, CreateTopicsRequestFilter, CreateTopicsResponseFilter,
        DeleteTopicsRequestFilter, DeleteTopicsResponseFilter, DescribeGroupsRequestFilter, DescribeGroupsResponseFilter {

    @Override
    public void onProduceRequest(RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
    }

    @Override
    public void onProduceResponse(ResponseHeaderData header, ProduceResponseData response, KrpcFilterContext context) {
    }

    @Override
    public void onCreateTopicsRequest(RequestHeaderData header, CreateTopicsRequestData request, KrpcFilterContext context) {

    }

    @Override
    public void onCreateTopicsResponse(ResponseHeaderData header, CreateTopicsResponseData response, KrpcFilterContext context) {

    }

    @Override
    public void onDeleteTopicsRequest(RequestHeaderData header, DeleteTopicsRequestData request, KrpcFilterContext context) {

    }

    @Override
    public void onDeleteTopicsResponse(ResponseHeaderData header, DeleteTopicsResponseData response, KrpcFilterContext context) {

    }

    @Override
    public void onDescribeGroupsRequest(RequestHeaderData header, DescribeGroupsRequestData request, KrpcFilterContext context) {

    }

    @Override
    public void onDescribeGroupsResponse(ResponseHeaderData header, DescribeGroupsResponseData response, KrpcFilterContext context) {

    }
}
