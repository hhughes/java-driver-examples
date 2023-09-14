package com.datastax.astra.driver.examples.beans;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import io.netty.channel.Channel;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectionsNode implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final AtomicInteger ZERO = new AtomicInteger(0);

    private final String hostId;
    private final String datacenter;
    private final String endpoint;
    private final String broadcastRpcAddress;
    private final List<String> openConnections;
    private final NodeState state;
    private final String controlConnection;
    private final boolean active;
    private final int requestsProcessed;

    @ConstructorProperties({"hostId", "datacenter", "endpoint", "broadcastRpcAddress", "openConnections", "state", "controlConnection", "active", "requestsProcessed"})
    public ConnectionsNode(String hostId,
                           String datacenter,
                           String endpoint,
                           String broadcastRpcAddress,
                           List<String> openConnections,
                           NodeState state,
                           String controlConnection,
                           boolean active,
                           int requestsProcessed) {
        this.hostId = hostId;
        this.datacenter = datacenter;
        this.endpoint = endpoint;
        this.broadcastRpcAddress = broadcastRpcAddress;
        this.openConnections = openConnections;
        this.state = state;
        this.controlConnection = controlConnection;
        this.active = active;
        this.requestsProcessed = requestsProcessed;
    }

    public ConnectionsNode(Node node, InternalDriverContext context, Map<Node, AtomicInteger> requestCounts) {
        this(Optional.ofNullable(node.getHostId()).map(Objects::toString).orElse("null"),
                node.getDatacenter(),
                node.getEndPoint().toString(),
                node.getBroadcastRpcAddress().map(Objects::toString).orElse("null"),
                openConnections(node, context),
                node.getState(),
                controlConnectionString(node, context).orElse("false"),
                context.getLoadBalancingPolicyWrapper().newQueryPlan().stream().anyMatch(n -> n.equals(node)),
                requestCounts.getOrDefault(node, ZERO).get());
    }

    private static Optional<String> controlConnectionString(Node node, InternalDriverContext context) {
        return Stream.of(context.getControlConnection())
                .map(ControlConnection::channel)
                .filter(driverChannel -> node.getEndPoint().equals(driverChannel.getEndPoint()))
                .map(Connections::getChannel)
                .map(channel -> String.format("%s#%s", channel.id(), channel.remoteAddress()))
                .findAny();
    }

    private static List<String> openConnections(Node node, InternalDriverContext context) {
        ChannelPool channelPool = context.getPoolManager().getPools().get(node);
        try {
            List<String> connections = new ArrayList<>();
            controlConnectionString(node, context).ifPresent(connections::add);
            Object channelSet = FieldUtils.readField(channelPool, "channels", true);
            Stream.of((DriverChannel[]) FieldUtils.readField(channelSet, "channels", true))
                    .map(Connections::getChannel)
                    .map(channel -> String.format("%s#%s", channel.id(), channel.remoteAddress()))
                    .forEach(connections::add);
            return connections;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public String getHostId() {
        return hostId;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getBroadcastRpcAddress() {
        return broadcastRpcAddress;
    }

    public List<String> getOpenConnections() {
        return openConnections;
    }

    public NodeState getState() {
        return state;
    }

    public String getControlConnection() {
        return controlConnection;
    }

    public boolean isActive() {
        return active;
    }

    @Override
    public String toString() {
        return "ConnectionsNode{" +
                "hostId='" + hostId + '\'' +
                ", datacenter='" + datacenter + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", broadcastRpcAddress='" + broadcastRpcAddress + '\'' +
                ", openConnections=" + openConnections +
                ", state=" + state +
                ", controlConnection=" + controlConnection +
                ", active=" + active +
                '}';
    }

    public int getRequestsProcessed() {
        return requestsProcessed;
    }
}
