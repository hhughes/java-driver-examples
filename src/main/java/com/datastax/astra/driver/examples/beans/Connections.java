package com.datastax.astra.driver.examples.beans;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.config.cloud.CloudConfig;
import com.datastax.oss.driver.internal.core.config.cloud.CloudConfigFactory;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.NodeInfo;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import io.netty.channel.Channel;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class Connections implements ConnectionsMXBean {

    private static final Logger LOG = LoggerFactory.getLogger(Connections.class);

    final CqlSession session;
    final Map<Node, AtomicInteger> requestCounts;
    final String scb;
    final List<ChannelEventNotification> channelEvents = new CopyOnWriteArrayList<>();
    final List<NodeStateEventNotification> nodeStateEvents = new CopyOnWriteArrayList<>();

    public Connections(CqlSession session, Map<Node, AtomicInteger> requestCounts, String scb) {
        this.session = session;
        this.requestCounts = requestCounts;
        this.scb = scb;

        InternalDriverContext context = getContext();

        /* context.getEventBus().register(ChannelEvent.class, event -> {
            LOG.info("sending channel event notification: {}", event);
            channelEvents.add(new ChannelEventNotification(new Date(), new ConnectionsNode(event.node, context, Collections.emptyMap()), event.type));
        });
        context.getEventBus().register(NodeStateEvent.class, event -> {
            LOG.info("sending node state event notification: {}", event);
            nodeStateEvents.add(new NodeStateEventNotification(new Date(), new ConnectionsNode(event.node, context, Collections.emptyMap()), event.oldState, event.newState));
        }); */
    }

    private InternalDriverContext getContext() {
        return (InternalDriverContext) session.getContext();
    }

    @Override
    public List<ConnectionsNode> getNodes() {
        InternalDriverContext context = getContext();
        return session.getMetadata().getNodes().values().stream()
                .map(node -> new ConnectionsNode(node, context, requestCounts))
                .collect(Collectors.toList());
    }

    /* @Override
    public List<ChannelEventNotification> getChannelEvents() {
        return channelEvents;
    }

    @Override
    public List<NodeStateEventNotification> getNodeStateEvents() {
        return nodeStateEvents;
    } */

    @Override
    public void closeControlConnection() {
        getChannel(getContext().getControlConnection().channel()).close();
    }

    @Override
    public void suggestRemove(String broadcastRpcAddress) {
        getContext().getEventBus().fire(TopologyEvent.suggestRemoved(createAddress(broadcastRpcAddress)));
    }

    @Override
    public void suggestAdd(String broadcastRpcAddress) {
        getContext().getEventBus().fire(TopologyEvent.suggestAdded(createAddress(broadcastRpcAddress)));
    }

    @Override
    public void forceDown(String broadcastRpcAddress) {
        getContext().getEventBus().fire(TopologyEvent.forceDown(createAddress(broadcastRpcAddress)));
    }

    @Override
    public void forceUp(String broadcastRpcAddress) {
        getContext().getEventBus().fire(TopologyEvent.forceUp(createAddress(broadcastRpcAddress)));
    }

    @Override
    public List<ConnectionsNode> refreshNodeList() {
        List<ConnectionsNode> nodes = new ArrayList<>();
        for (NodeInfo nodeInfo : getContext().getTopologyMonitor().refreshNodeList().toCompletableFuture().join()) {
            nodes.add(new ConnectionsNode(nodeInfo.getHostId().toString(), nodeInfo.getDatacenter(), nodeInfo.getEndPoint().toString(), nodeInfo.getBroadcastRpcAddress().toString(), Collections.emptyList(), null, "", false, 0));
        }
        return nodes;
    }

    @Override
    public void closeConnection(String broadcastRpcAddress) {
        Node node = findNode(broadcastRpcAddress);
        InternalDriverContext context = getContext();
        DriverChannel driverChannel = context.getPoolManager().getPools().get(node).next();
        getChannel(driverChannel).close();
    }

    @Override
    public String fetchSniEndpoints() {
        try {
            CloudConfig cloudConfig = new CloudConfigFactory().createCloudConfig(Paths.get(this.scb).toUri().toURL().openStream());
            return String.format("SNI Endpoint: %s/metadata\nContact Points: [%s]", cloudConfig.getProxyAddress(),
                    cloudConfig.getEndPoints().stream().map(Objects::toString).collect(Collectors.joining(",\n")));
        } catch (IOException | GeneralSecurityException e) {
            LOG.error("failed to parse scb", e);
            throw new RuntimeException(e);
        }
    }

    private InetSocketAddress createAddress(String broadcastRpcAddress) {
        String[] split = broadcastRpcAddress.split(":");
        return new InetSocketAddress(split[0].replaceAll("/", ""), Integer.parseInt(split[1]));
    }

    private Node findNode(String broadcastRpcAddress) {
        InternalDriverContext context = getContext();
        InetSocketAddress address = createAddress(broadcastRpcAddress);

        return context.getMetadataManager().getMetadata().getNodes().values().stream()
                .filter(n -> n.getBroadcastAddress().map(addr -> address.getHostName().equals(addr.getHostName())).orElse(false)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Could not find host with address: " + broadcastRpcAddress));
    }

    public static Channel getChannel(DriverChannel driverChannel) {
        try {
            return ((Channel) FieldUtils.readField(driverChannel, "channel", true));
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getChannelId(Channel channel) {
        return String.format("0x%s", channel.id());
    }
}
