package database;


import graph.Edge;

public interface ISilo {
    /**
     * Returns the schema hash if known, otherwise null.
     * @param nodeID
     * @return
     */
    Integer getPreviousElementID(String nodeID);

    /**
     * Removes the link between node and schema element, returns the number
     * of still active links to this schema element.
     * @param nodeID
     * @param schemaHash
     * @return
     */
    Integer removeNodeFromSchemaElement(String nodeID, Integer schemaHash);

    /**
     * Add the link between node and schema element, returns the number of now active links.
     *
     * @param nodeID
     * @param schemaHash
     * @return
     */
    Integer addNodeFromSchemaElement(String nodeID, Integer schemaHash);


    /**
     * Returns the schema hash if known, otherwise null.
     * @param edgeID
     * @return
     */
    Integer getPreviousLinkID(Integer edgeID);

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
     * @param start
     * @param end
     * @param linkHash
     * @return
     */
    Integer addEdgeFromSchemaEdge(Integer edgeID, Integer linkHash);


}
