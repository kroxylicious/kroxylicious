package io.kroxylicious.proxy.internal.net;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.service.HostPort;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test to verify that bootstrap server selection distributes across multiple servers
 * instead of always selecting the first one.
 */
class BootstrapServerSelectionTest {
    
    @Test
    void shouldSelectDifferentBootstrapServersOverTime() {
        // Arrange
        List<HostPort> bootstrapServers = List.of(
            HostPort.parse("server1:9092"),
            HostPort.parse("server2:9092"), 
            HostPort.parse("server3:9092")
        );
        
        Set<HostPort> selectedServers = new HashSet<>();
        
        // Act - simulate different time values to test distribution
        for (long timeMillis = 0; timeMillis < 100; timeMillis++) {
            Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(timeMillis), ZoneOffset.UTC);
            HostPort selected = EndpointRegistry.selectBootstrapServer(bootstrapServers, fixedClock);
            selectedServers.add(selected);
        }
        
        // Assert - should have selected different servers (all 3 with time 0-99)
        assertThat(selectedServers.size()).isEqualTo(3)
            .describedAs("Should select all different bootstrap servers over time range");
        assertThat(selectedServers).containsExactlyInAnyOrder(
            HostPort.parse("server1:9092"),
            HostPort.parse("server2:9092"),
            HostPort.parse("server3:9092")
        );
    }
    
    @Test 
    void shouldReturnSingleServerWhenOnlyOneProvided() {
        // Arrange
        List<HostPort> singleServer = List.of(HostPort.parse("server1:9092"));
        Clock anyClock = Clock.fixed(Instant.ofEpochMilli(12345), ZoneOffset.UTC);
        
        // Act
        HostPort selected = EndpointRegistry.selectBootstrapServer(singleServer, anyClock);
        
        // Assert
        assertThat(selected).isEqualTo(HostPort.parse("server1:9092"));
    }
    
    @Test
    void shouldDistributeEvenlyAcrossServers() {
        // Arrange
        List<HostPort> bootstrapServers = List.of(
            HostPort.parse("server1:9092"),
            HostPort.parse("server2:9092")
        );
        
        // Act - test specific time values that should select each server
        Clock clock0 = Clock.fixed(Instant.ofEpochMilli(0), ZoneOffset.UTC);  // 0 % 2 = 0 -> server1
        Clock clock1 = Clock.fixed(Instant.ofEpochMilli(1), ZoneOffset.UTC);  // 1 % 2 = 1 -> server2
        Clock clock2 = Clock.fixed(Instant.ofEpochMilli(2), ZoneOffset.UTC);  // 2 % 2 = 0 -> server1
        
        HostPort selected0 = EndpointRegistry.selectBootstrapServer(bootstrapServers, clock0);
        HostPort selected1 = EndpointRegistry.selectBootstrapServer(bootstrapServers, clock1);
        HostPort selected2 = EndpointRegistry.selectBootstrapServer(bootstrapServers, clock2);
        
        // Assert
        assertThat(selected0).isEqualTo(HostPort.parse("server1:9092"));
        assertThat(selected1).isEqualTo(HostPort.parse("server2:9092"));
        assertThat(selected2).isEqualTo(HostPort.parse("server1:9092"));
    }
}
