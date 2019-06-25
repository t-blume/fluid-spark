package database;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class GraphSiloJava implements Serializable {

    private HashMap<Integer, Integer> nodeIDtoSchemaHash = new HashMap<>();
    private HashMap<Integer, Set<Integer>> schemaHashtoNodeIDs = new HashMap<>();


    private HashMap<Integer, Integer> edgeIDtoLinkHash = new HashMap<>();
    private HashMap<Integer, Set<Integer>> linkHashtoEdgeIDs = new HashMap<>();

    /**
     * return id or null if not present
     *
     * @param nodeID
     * @return
     */
    public Integer getPreviousElementID(Integer nodeID) {
        return nodeIDtoSchemaHash.get(nodeID);
    }

    
    public Integer removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash) {
        Set<Integer> nodeIDs = schemaHashtoNodeIDs.get(schemaHash);
        if (nodeIDs != null) {
            nodeIDs.remove(nodeID);
            if (nodeIDs.size() <= 0)
                schemaHashtoNodeIDs.remove(schemaHash);
            else
                schemaHashtoNodeIDs.put(schemaHash, nodeIDs); //TODO needed?

            return nodeIDs.size();
        } else
            return 0;
    }

    
    public Integer addNodeFromSchemaElement(Integer nodeID, Integer schemaHash) {
        Set<Integer> nodeIDs = schemaHashtoNodeIDs.get(schemaHash);
        if (nodeIDs == null)
            nodeIDs = new HashSet<>();

        nodeIDs.add(nodeID);
        schemaHashtoNodeIDs.put(schemaHash, nodeIDs);
        return nodeIDs.size();
    }

    
    public void touch(Integer nodeID) {

    }

    
    public Integer getPreviousLinkID(Integer edgeID) {
        return edgeIDtoLinkHash.get(edgeID);
    }

    
    public Integer removeEdgeFromSchemaEdge(Integer edgeID, Integer linkHash) {
        Set<Integer> edgeIDs = linkHashtoEdgeIDs.get(linkHash);
        if (edgeIDs != null) {
            edgeIDs.remove(edgeID);
            if (edgeIDs.size() <= 0)
                linkHashtoEdgeIDs.remove(linkHash);
            else
                linkHashtoEdgeIDs.put(linkHash, edgeIDs); //TODO needed?

            return edgeIDs.size();
        } else
            return 0;
    }

    
    public Integer addEdgeFromSchemaEdge(Integer edgeID, Integer linkHash) {
        Set<Integer> edgeIDs = linkHashtoEdgeIDs.get(linkHash);
        if (edgeIDs == null)
            edgeIDs = new HashSet<>();

        edgeIDs.add(edgeID);
        linkHashtoEdgeIDs.put(linkHash, edgeIDs);
        return edgeIDs.size();
    }

}
