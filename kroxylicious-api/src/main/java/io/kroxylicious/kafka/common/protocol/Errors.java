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
package io.kroxylicious.kafka.common.protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 *
 * Note that client library will convert an unknown error code to the non-retriable UnknownServerException if the client library
 * version is old and does not recognize the newly-added error code. Therefore when a new server-side error is added,
 * we may need extra logic to convert the new error code to another existing error code before sending the response back to
 * the client if the request version suggests that the client may not recognize the new error code.
 *
 * Do not add exceptions that occur only on the client or only on the server here.
 *
 * @see io.kroxylicious.kafka.common.network.SslTransportLayer
 */
public enum Errors {
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error when processing the request."),
    NONE(0, null),
    OFFSET_OUT_OF_RANGE(1, "The requested offset is not within the range of offsets maintained by the server."),
    CORRUPT_MESSAGE(2, "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt."),
    UNKNOWN_TOPIC_OR_PARTITION(3, "This server does not host this topic-partition."),
    INVALID_FETCH_SIZE(4, "The requested fetch size is invalid."),
    LEADER_NOT_AVAILABLE(5, "There is no leader for this topic-partition as we are in the middle of a leadership election."),
    NOT_LEADER_OR_FOLLOWER(6, "For requests intended only for the leader, this error indicates that the broker is not the current leader. " +
            "For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition."),
    REQUEST_TIMED_OUT(7, "The request timed out."),
    BROKER_NOT_AVAILABLE(8, "The broker is not available."),
    REPLICA_NOT_AVAILABLE(9, "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests " +
            "intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition."),
    MESSAGE_TOO_LARGE(10, "The request included a message larger than the max message size the server will accept."),
    STALE_CONTROLLER_EPOCH(11, "The controller moved to another broker."),
    OFFSET_METADATA_TOO_LARGE(12, "The metadata field of the offset request was too large."),
    NETWORK_EXCEPTION(13, "The server disconnected before a response was received."),
    COORDINATOR_LOAD_IN_PROGRESS(14, "The coordinator is loading and hence can't process requests."),
    COORDINATOR_NOT_AVAILABLE(15, "The coordinator is not available."),
    NOT_COORDINATOR(16, "This is not the correct coordinator."),
    INVALID_TOPIC_EXCEPTION(17, "The request attempted to perform an operation on an invalid topic."),
    RECORD_LIST_TOO_LARGE(18, "The request included message batch larger than the configured segment size on the server."),
    NOT_ENOUGH_REPLICAS(19, "Messages are rejected since there are fewer in-sync replicas than required."),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20, "Messages are written to the log, but to fewer in-sync replicas than required."),
    INVALID_REQUIRED_ACKS(21, "Produce request specified an invalid value for required acks."),
    ILLEGAL_GENERATION(22, "Specified group generation id is not valid."),
    INCONSISTENT_GROUP_PROTOCOL(23,
            "The group member's supported protocols are incompatible with those of existing members " +
                    "or first group member tried to join with empty protocol type or empty protocol list."),
    INVALID_GROUP_ID(24, "The group id is invalid."),
    UNKNOWN_MEMBER_ID(25, "The coordinator is not aware of this member."),
    INVALID_SESSION_TIMEOUT(26,
            "The session timeout is not within the range allowed by the broker " +
                    "(as configured by group.min.session.timeout.ms and group.max.session.timeout.ms)."),
    REBALANCE_IN_PROGRESS(27, "The group is rebalancing, so a rejoin is needed."),
    INVALID_COMMIT_OFFSET_SIZE(28, "The committing offset data size is not valid."),
    TOPIC_AUTHORIZATION_FAILED(29, "Topic authorization failed."),
    GROUP_AUTHORIZATION_FAILED(30, "Group authorization failed."),
    CLUSTER_AUTHORIZATION_FAILED(31, "Cluster authorization failed."),
    INVALID_TIMESTAMP(32, "The timestamp of the message is out of acceptable range."),
    UNSUPPORTED_SASL_MECHANISM(33, "The broker does not support the requested SASL mechanism."),
    ILLEGAL_SASL_STATE(34, "Request is not valid given the current SASL state."),
    UNSUPPORTED_VERSION(35, "The version of API is not supported."),
    TOPIC_ALREADY_EXISTS(36, "Topic with this name already exists."),
    INVALID_PARTITIONS(37, "Number of partitions is below 1."),
    INVALID_REPLICATION_FACTOR(38, "Replication factor is below 1 or larger than the number of available brokers."),
    INVALID_REPLICA_ASSIGNMENT(39, "Replica assignment is invalid."),
    INVALID_CONFIG(40, "Configuration is invalid."),
    NOT_CONTROLLER(41, "This is not the correct controller for this cluster."),
    INVALID_REQUEST(42, "This most likely occurs because of a request being malformed by the " +
            "client library or the message was sent to an incompatible broker. See the broker logs " +
            "for more details."),
    UNSUPPORTED_FOR_MESSAGE_FORMAT(43, "The message format version on the broker does not support the request."),
    POLICY_VIOLATION(44, "Request parameters do not satisfy the configured policy."),
    OUT_OF_ORDER_SEQUENCE_NUMBER(45, "The broker received an out of order sequence number."),
    DUPLICATE_SEQUENCE_NUMBER(46, "The broker received a duplicate sequence number."),
    INVALID_PRODUCER_EPOCH(47, "Producer attempted to produce with an old epoch."),
    INVALID_TXN_STATE(48, "The producer attempted a transactional operation in an invalid state."),
    INVALID_PRODUCER_ID_MAPPING(49, "The producer attempted to use a producer id which is not currently assigned to " +
            "its transactional id."),
    INVALID_TRANSACTION_TIMEOUT(50, "The transaction timeout is larger than the maximum value allowed by " +
            "the broker (as configured by transaction.max.timeout.ms)."),
    CONCURRENT_TRANSACTIONS(51, "The producer attempted to update a transaction " +
            "while another concurrent operation on the same transaction was ongoing."),
    TRANSACTION_COORDINATOR_FENCED(52, "Indicates that the transaction coordinator sending a WriteTxnMarker " +
            "is no longer the current coordinator for a given producer."),
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED(53, "Transactional Id authorization failed."),
    SECURITY_DISABLED(54, "Security features are disabled."),
    OPERATION_NOT_ATTEMPTED(55, "The broker did not attempt to execute this operation. This may happen for " +
            "batched RPCs where some operations in the batch failed, causing the broker to respond without " +
            "trying the rest."),
    KAFKA_STORAGE_ERROR(56, "Disk error when trying to access log file on the disk."),
    LOG_DIR_NOT_FOUND(57, "The user-specified log directory is not found in the broker config."),
    SASL_AUTHENTICATION_FAILED(58, "SASL Authentication failed."),
    UNKNOWN_PRODUCER_ID(59, "This exception is raised by the broker if it could not locate the producer metadata " +
            "associated with the producerId in question. This could happen if, for instance, the producer's records " +
            "were deleted because their retention time had elapsed. Once the last records of the producerId are " +
            "removed, the producer's metadata is removed from the broker, and future appends by the producer will " +
            "return this exception."),
    REASSIGNMENT_IN_PROGRESS(60, "A partition reassignment is in progress."),
    DELEGATION_TOKEN_AUTH_DISABLED(61, "Delegation Token feature is not enabled."),
    DELEGATION_TOKEN_NOT_FOUND(62, "Delegation Token is not found on server."),
    DELEGATION_TOKEN_OWNER_MISMATCH(63, "Specified Principal is not valid Owner/Renewer."),
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED(64, "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL " +
            "channels and on delegation token authenticated channels."),
    DELEGATION_TOKEN_AUTHORIZATION_FAILED(65, "Delegation Token authorization failed."),
    DELEGATION_TOKEN_EXPIRED(66, "Delegation Token is expired."),
    INVALID_PRINCIPAL_TYPE(67, "Supplied principalType is not supported."),
    NON_EMPTY_GROUP(68, "The group is not empty."),
    GROUP_ID_NOT_FOUND(69, "The group id does not exist."),
    FETCH_SESSION_ID_NOT_FOUND(70, "The fetch session ID was not found."),
    INVALID_FETCH_SESSION_EPOCH(71, "The fetch session epoch is invalid."),
    LISTENER_NOT_FOUND(72, "There is no listener on the leader broker that matches the listener on which " +
            "metadata request was processed."),
    TOPIC_DELETION_DISABLED(73, "Topic deletion is disabled."),
    FENCED_LEADER_EPOCH(74, "The leader epoch in the request is older than the epoch on the broker."),
    UNKNOWN_LEADER_EPOCH(75, "The leader epoch in the request is newer than the epoch on the broker."),
    UNSUPPORTED_COMPRESSION_TYPE(76, "The requesting client does not support the compression type of given partition."),
    STALE_BROKER_EPOCH(77, "Broker epoch has changed."),
    OFFSET_NOT_AVAILABLE(78, "The leader high watermark has not caught up from a recent leader " +
            "election so the offsets cannot be guaranteed to be monotonically increasing."),
    MEMBER_ID_REQUIRED(79, "The group member needs to have a valid member id before actually entering a consumer group."),
    PREFERRED_LEADER_NOT_AVAILABLE(80, "The preferred leader was not available."),
    GROUP_MAX_SIZE_REACHED(81, "The group has reached its maximum size."),
    FENCED_INSTANCE_ID(82, "The broker rejected this static consumer since " +
            "another consumer with the same group.instance.id has registered with a different member.id."),
    ELIGIBLE_LEADERS_NOT_AVAILABLE(83, "Eligible topic partition leaders are not available."),
    ELECTION_NOT_NEEDED(84, "Leader election not needed for topic partition."),
    NO_REASSIGNMENT_IN_PROGRESS(85, "No partition reassignment is in progress."),
    GROUP_SUBSCRIBED_TO_TOPIC(86, "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it."),
    INVALID_RECORD(87, "This record has failed the validation on broker and hence will be rejected."),
    UNSTABLE_OFFSET_COMMIT(88, "There are unstable offsets that need to be cleared."),
    THROTTLING_QUOTA_EXCEEDED(89, "The throttling quota has been exceeded."),
    PRODUCER_FENCED(90, "There is a newer producer with the same transactionalId " +
            "which fences the current one."),
    RESOURCE_NOT_FOUND(91, "A request illegally referred to a resource that does not exist."),
    DUPLICATE_RESOURCE(92, "A request illegally referred to the same resource twice."),
    UNACCEPTABLE_CREDENTIAL(93, "Requested credential would not meet criteria for acceptability."),
    INCONSISTENT_VOTER_SET(94, "Indicates that the either the sender or recipient of a " +
            "voter-only request is not one of the expected voters."),
    INVALID_UPDATE_VERSION(95, "The given update version was invalid."),
    FEATURE_UPDATE_FAILED(96, "Unable to update finalized features due to an unexpected server error."),
    PRINCIPAL_DESERIALIZATION_FAILURE(97, "Request principal deserialization failed during forwarding. " +
            "This indicates an internal error on the broker cluster security setup."),
    SNAPSHOT_NOT_FOUND(98, "Requested snapshot was not found."),
    POSITION_OUT_OF_RANGE(99, "Requested position is not greater than or equal to zero, and less than the size of the snapshot."),
    UNKNOWN_TOPIC_ID(100, "This server does not host this topic ID."),
    DUPLICATE_BROKER_REGISTRATION(101, "This broker ID is already in use."),
    BROKER_ID_NOT_REGISTERED(102, "The given broker ID was not registered."),
    INCONSISTENT_TOPIC_ID(103, "The log's topic ID did not match the topic ID in the request."),
    INCONSISTENT_CLUSTER_ID(104, "The clusterId in the request does not match that found on the server."),
    TRANSACTIONAL_ID_NOT_FOUND(105, "The transactionalId could not be found."),
    FETCH_SESSION_TOPIC_ID_ERROR(106, "The fetch session encountered inconsistent topic ID usage."),
    INELIGIBLE_REPLICA(107, "The new ISR contains at least one ineligible replica."),
    NEW_LEADER_ELECTED(108, "The AlterPartition request successfully updated the partition state but the leader has changed."),
    OFFSET_MOVED_TO_TIERED_STORAGE(109, "The requested offset is moved to tiered storage."),
    FENCED_MEMBER_EPOCH(110, "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin."),
    UNRELEASED_INSTANCE_ID(111, "The instance ID is still used by another member in the consumer group. That member must leave first."),
    UNSUPPORTED_ASSIGNOR(112, "The assignor or its version range is not supported by the consumer group."),
    STALE_MEMBER_EPOCH(113, "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API."),
    MISMATCHED_ENDPOINT_TYPE(114, "The request was sent to an endpoint of the wrong type."),
    UNSUPPORTED_ENDPOINT_TYPE(115, "This endpoint type is not supported yet."),
    UNKNOWN_CONTROLLER_ID(116, "This controller ID is not known."),
    UNKNOWN_SUBSCRIPTION_ID(117, "Client sent a push telemetry request with an invalid or outdated subscription ID."),
    TELEMETRY_TOO_LARGE(118, "Client sent a push telemetry request larger than the maximum size the broker will accept."),
    INVALID_REGISTRATION(119, "The controller has considered the broker registration to be invalid."),
    TRANSACTION_ABORTABLE(120, "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID."),
    INVALID_RECORD_STATE(121, "The record state is invalid. The acknowledgement of delivery could not be completed."),
    SHARE_SESSION_NOT_FOUND(122, "The share session was not found."),
    INVALID_SHARE_SESSION_EPOCH(123, "The share session epoch is invalid."),
    FENCED_STATE_EPOCH(124, "The share coordinator rejected the request because the share-group state epoch did not match."),
    INVALID_VOTER_KEY(125, "The voter key doesn't match the receiving replica's key."),
    DUPLICATE_VOTER(126, "The voter is already part of the set of voters."),
    VOTER_NOT_FOUND(127, "The voter is not part of the set of voters."),
    INVALID_REGULAR_EXPRESSION(128, "The regular expression is not valid."),
    REBOOTSTRAP_REQUIRED(129, "Client metadata is stale. The client should rebootstrap to obtain new metadata."),
    STREAMS_INVALID_TOPOLOGY(130, "The supplied topology is invalid."),
    STREAMS_INVALID_TOPOLOGY_EPOCH(131, "The supplied topology epoch is invalid."),
    STREAMS_TOPOLOGY_FENCED(132, "The supplied topology epoch is outdated."),
    SHARE_SESSION_LIMIT_REACHED(133, "The limit of share sessions has been reached.");

    private static final Logger log = LoggerFactory.getLogger(Errors.class);
    private static final Map<Short, Errors> CODE_TO_ERROR = new HashMap<>();

    static {
        for (Errors error : Errors.values()) {
            if (CODE_TO_ERROR.put(error.code(), error) != null)
                throw new ExceptionInInitializerError("Code " + error.code() + " for error " +
                        error + " has already been used");
        }
    }

    private final short code;

    private final String message;

    Errors(int code, String defaultMessage) {
        this.code = (short) code;
        this.message = defaultMessage;
    }

    /**
     * The error code for the exception
     */
    public short code() {
        return this.code;
    }

    /**
     * Get a friendly description of the error (if one is available).
     * @return the error message
     */
    public String message() {
        return this.message;
    }

    /**
     * Throw the exception if there is one
     */
    public static Errors forCode(short code) {
        Errors error = CODE_TO_ERROR.get(code);
        if (error != null) {
            return error;
        }
        else {
            log.warn("Unexpected error code: {}.", code);
            return UNKNOWN_SERVER_ERROR;
        }
    }

    /**
     * Check if a Throwable is a commonly wrapped exception type (e.g. `CompletionException`) and return
     * the cause if so. This is useful to handle cases where exceptions may be raised from a future or a
     * completion stage (as might be the case for requests sent to the controller in `ControllerApis`).
     *
     * @param t The Throwable to check
     * @return The throwable itself or its cause if it is an instance of a commonly wrapped exception type
     */
    public static Throwable maybeUnwrapException(Throwable t) {
        if (t instanceof CompletionException || t instanceof ExecutionException) {
            return t.getCause();
        }
        else {
            return t;
        }
    }
}
