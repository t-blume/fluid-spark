package incremental;

import database.ISilo;
import graph.Edge;
import scala.Tuple2;
import schema.ISchemaElement;

import java.util.Set;

public class IGSI {

    private ISilo silo;

    public IGSI(ISilo silo) {
        this.silo = silo;
    }

    /**
     * try to add a set of schema elements
     * assumption: each schema element is the same, the difference is the instance
     *
     * @param schemaElements
     */
    public void tryAdd(Set<ISchemaElement> schemaElements) {
        ISchemaElement finalSchemaElement = null;
        //update instance - schema relations, delete if necessary
        for (ISchemaElement schemaElement : schemaElements) {
            //create schema element from first one
            if (finalSchemaElement == null)
                finalSchemaElement = schemaElement;


            Set<Tuple2<Edge, Edge>> schemaEdges = schemaElement.getSchemaEdges();
            //get vertexID of all outgoing edges
            String vertexID = schemaEdges.iterator().next()._1.start;
            //check if previously known
            Integer schemaHash = silo.getPreviousElementID(vertexID);
            if (schemaHash != null) {
                //instance (vertex) was known before
                if (schemaHash != schemaElement.hashCode()) {
                    //it was something else before, remove link to old schema element
                    Integer activeLinks = silo.removeNodeFromSchemaElement(vertexID, schemaHash);
                    //add link to new schema element
                    silo.addNodeFromSchemaElement(vertexID, schemaElement.hashCode());
                    //check if old schema element is still needed, delete otherwise from schema graph summary
                    if (activeLinks <= 0)
                        deleteSchemaElement(schemaHash);
                }
            }
            //TODO: maybe move to inner loop above
            for (Tuple2<Edge, Edge> edgeTuple : schemaEdges) {
                Integer linkHash = silo.getPreviousLinkID(edgeTuple._1);
                if (linkHash != null) {
                    //instance level edge was known before
                    if (linkHash != edgeTuple._2.hashCode()) {
                        //it was something else before, remove link to old schema edge
                        Integer activeLinks = silo.removeEdgeFromSchemaEdge(edgeTuple._1.start, edgeTuple._1.end, linkHash);
                        //add link to new schema edge
                        silo.addEdgeFromSchemaEdge(edgeTuple._1.start, edgeTuple._1.end, edgeTuple._2.hashCode());
                        //check if old schema edge is still needed, delete otherwise from schema graph summary
                        if (activeLinks <= 0)
                            deleteSchemaEdge(linkHash);
                    }
                }
            }
        }

        //if it is something new, actually write it to the schema graph summary
        if (!exists(finalSchemaElement))
            writeSchemaElementWithEdges(finalSchemaElement);


    }

    private Edge getSchemaEdge(Integer start, Integer end) {
        return null; //TODO
    }

    /**
     * delete the schema element.
     *
     * @param schemaHash
     */
    private void deleteSchemaElement(Integer schemaHash) {
        //TODO:
    }

    /**
     * delete the schema edge.
     *
     * @param linkHash
     */
    private void deleteSchemaEdge(Integer linkHash) {
        //TODO:
    }


    private boolean exists(ISchemaElement schemaElement) {
        return false; //check if schema element is in graph summary
    }

    /**
     * actually write the schema element and its relations
     *
     * @param schemaElement
     */
    private void writeSchemaElementWithEdges(ISchemaElement schemaElement) {

    }


    /**
     * delete the schema element.
     *
     * @param payloadHash
     */
    private void deletePayloadElement(Integer payloadHash) {
        //TODO:
    }

    /**
     * delete the schema edge.
     *
     * @param linkHash
     */
    private void deletePayloadEdge(Integer linkHash) {
        //TODO:
    }

}
