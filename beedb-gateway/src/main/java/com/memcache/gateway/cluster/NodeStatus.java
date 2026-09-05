package com.memcache.gateway.cluster;

/*
 * denotes status of each node
 * Integer (boxed types) are used to differentiate between 0 vs null (unknown)
 */
public record NodeStatus(
    Integer currItems,
    String raftNodeId,
    String raftRole,
    Long raftTerm,
    Integer raftCommitIndex,
    Integer raftLastApplied,
    String raftLeaderId,
    Integer raftLastIncludedIndex,
    Integer raftLogSize
){

    public static NodeStatus of(Integer currItems, String raftNodeId, String raftRole, Long raftTerm, Integer raftCommitIndex, Integer raftLastApplied, String raftLeaderId, Integer raftLastIncludedIndex, Integer raftLogSize) {
        return new NodeStatus(currItems, raftNodeId, raftRole, raftTerm, raftCommitIndex, raftLastApplied, raftLeaderId, raftLastIncludedIndex, raftLogSize);
    }
    public static NodeStatus unreachable(String nodeId, String address) {
        return new NodeStatus(null, nodeId, null, null, null, null, null, null, null);
    }

    public static String getRole(NodeStatus nodeStatus) {
        return nodeStatus.raftRole();
    }
}
