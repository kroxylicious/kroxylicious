/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.tester;

import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KroxyliciousTestersCleanupTest {

    @Test
    void shouldRemoveShareGroup() {
        // Given
        var admin = mock(Admin.class);
        var result = mock(DeleteConsumerGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteConsumerGroups(List.of("group"))).thenReturn(result);

        // When
        KroxyliciousTestersTest.removeShareGroup(admin, "group");

        // Then
        verify(admin).deleteConsumerGroups(List.of("group"));
    }

    @Test
    void shouldAllowShareGroupThatWasNeverCreated() {
        // Given
        var admin = mock(Admin.class);
        var result = mock(DeleteConsumerGroupsResult.class);
        var deletion = new KafkaFutureImpl<Void>();
        deletion.completeExceptionally(new GroupIdNotFoundException("Group was never created"));
        when(result.all()).thenReturn(deletion);
        when(admin.deleteConsumerGroups(List.of("group"))).thenReturn(result);

        // When
        KroxyliciousTestersTest.removeShareGroup(admin, "group");

        // Then
        verify(admin).deleteConsumerGroups(List.of("group"));
    }

    @Test
    void shouldFailOnOtherDeletionErrors() {
        // Given
        var admin = mock(Admin.class);
        var result = mock(DeleteConsumerGroupsResult.class);
        var deletion = new KafkaFutureImpl<Void>();
        deletion.completeExceptionally(new GroupAuthorizationException("Not authorized to delete group"));
        when(result.all()).thenReturn(deletion);
        when(admin.deleteConsumerGroups(List.of("group"))).thenReturn(result);

        // When
        var failure = catchThrowable(() -> KroxyliciousTestersTest.removeShareGroup(admin, "group"));

        // Then
        assertThat(failure)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(GroupAuthorizationException.class.getName());
    }
}
