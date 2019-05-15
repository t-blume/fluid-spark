package database;

import graph.Edge;

import java.io.Serializable;
import java.util.HashMap;

public class GraphSiloJava implements Serializable, ISilo {

    private HashMap<String, Integer> nodeIDtoSchemaHash = new HashMap<>();

    /**
     * return id or null if not present
     * @param nodeID
     * @return
     */
    public Integer getPreviousElementID(String nodeID) {
        return nodeIDtoSchemaHash.get(nodeID);
    }

    @Override
    public Integer removeNodeFromSchemaElement(String nodeID, Integer schemaHash) {
        return null;
    }

    @Override
    public Integer addNodeFromSchemaElement(String nodeID, Integer schemaHash) {
        return null;
    }

    @Override
    public Integer getPreviousLinkID(Edge edge) {
        return null;
    }

    @Override
    public Integer removeEdgeFromSchemaEdge(String start, String end, Integer linkHash) {
        return null;
    }

    @Override
    public Integer addEdgeFromSchemaEdge(String start, String end, Integer linkHash) {
        return null;
    }

}
