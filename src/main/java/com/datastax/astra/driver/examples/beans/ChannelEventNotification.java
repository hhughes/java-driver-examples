package com.datastax.astra.driver.examples.beans;

import com.datastax.oss.driver.internal.core.channel.ChannelEvent;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.Date;

public class ChannelEventNotification implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Date timestamp;
    private final ConnectionsNode node;
    private final ChannelEvent.Type type;

    @ConstructorProperties({"timestamp", "node", "type"})
    public ChannelEventNotification(Date timestamp, ConnectionsNode node, ChannelEvent.Type type) {
        this.timestamp = timestamp;
        this.node = node;
        this.type = type;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public ConnectionsNode getNode() {
        return node;
    }

    public ChannelEvent.Type getType() {
        return type;
    }

    @Override
    public String toString() {
        return "ChannelEventNotification{" +
                "timestamp=" + timestamp +
                ", node=" + node +
                ", type=" + type +
                '}';
    }
}
