package com.datastax.astra.driver.examples.beans;

import java.util.List;

public interface ConnectionsMXBean {
    List<ConnectionsNode> getNodes();
    // List<ChannelEventNotification> getChannelEvents();
    // List<NodeStateEventNotification> getNodeStateEvents();
    void closeControlConnection();
    void suggestRemove(String broadcastRpcAddress);
    void suggestAdd(String broadcastRpcAddress);
    void forceDown(String broadcastRpcAddress);
    void forceUp(String broadcastRpcAddress);
    List<ConnectionsNode> refreshNodeList();
    void closeConnection(String broadcastRpcAddress);
    String fetchSniEndpoints();
}
