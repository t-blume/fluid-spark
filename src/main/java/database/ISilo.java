package database;


import graph.Edge;

public interface ISilo {
    /**
     * Returns the schema hash if known, otherwise null.
     * @param nodeID
     * @return
     */
    Object getPreviousElementID(Integer nodeID);

    /**
     * Removes the link between node and schema element, returns the number
     * of still active links to this schema element.
     * @param nodeID
     * @param schemaHash
     * @return
     */
    Integer removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash);

    /**
     * Add the link between node and schema element, returns the number of now active links.
     *
     * @param nodeID
     * @param schemaHash
     * @return
     */
    Integer addNodeFromSchemaElement(Integer nodeID, Integer schemaHash);

    /**
     * touch the node to create a new timestamp
     * @param nodeID
     */
    void touch(Integer nodeID);

    /**
     * Returns the schema hash if known, otherwise null.
     * @param edgeID
     * @return
     */
    Object getPreviousLinkID(Integer edgeID);

    /**
     * Removes the link between node and schema element, returns the number
     * of still active links to this schema element.
     * @param edgeID
     * @param linkHash
     * @return
     */
    Integer removeEdgeFromSchemaEdge(Integer edgeID, Integer linkHash);

    /**
     * Add the link between node and schema element, returns the number of now active links.
     *
     * @param edgeID
     * @param linkHash
     * @return
     */
    Integer addEdgeFromSchemaEdge(Integer edgeID, Integer linkHash);


}
