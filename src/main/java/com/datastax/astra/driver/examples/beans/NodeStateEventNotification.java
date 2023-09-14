package com.datastax.astra.driver.examples.beans;

import com.datastax.oss.driver.api.core.metadata.NodeState;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.Date;

public class NodeStateEventNotification implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Date timestamp;
    private final ConnectionsNode node;
    private final NodeState oldState;
    private final NodeState newState;

    @ConstructorProperties({"timestamp", "node", "oldState", "newState"})
    public NodeStateEventNotification(Date timestamp, ConnectionsNode node, NodeState oldState, NodeState newState) {
        this.timestamp = timestamp;
        this.node = node;
        this.oldState = oldState;
        this.newState = newState;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public ConnectionsNode getNode() {
        return node;
    }

    public NodeState getOldState() {
        return oldState;
    }

    public NodeState getNewState() {
        return newState;
    }

    @Override
    public String toString() {
        return "NodeStateEventNotification{" +
                "timestamp=" + timestamp +
                ", node=" + node +
                ", oldState=" + oldState +
                ", newState=" + newState +
                '}';
    }
}
