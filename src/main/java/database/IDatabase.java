package database;

import schema.ISchemaElement;

public interface IDatabase {


    /**
     * delete the schema element.
     *
     * @param schemaHash
     */
    void deleteSchemaElement(Integer schemaHash);

    /**
     * delete the schema edge.
     *
     * @param linkHash
     */
    void deleteSchemaEdge(Integer linkHash);


    /**
     * check if this is in database already
     *
     * @param classString
     * @param schemaHash
     * @return
     */
    boolean exists(String classString, Integer schemaHash);

    /**
     * actually write the schema element and its relations
     *
     * @param schemaElement
     */
    void writeSchemaElementWithEdges(ISchemaElement schemaElement);


    /**
     * delete the schema element.
     *
     * @param payloadHash
     */
    void deletePayloadElement(Integer payloadHash);

    /**
     * delete the schema edge.
     *
     * @param linkHash
     */
    void deletePayloadEdge(Integer linkHash);


    /**
     * close db connection
     */
    void close();
}
