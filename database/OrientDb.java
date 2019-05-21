package database;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import scala.Serializable;
import scala.Tuple2;
import schema.ISchemaElement;

import java.util.Iterator;

import static database.Constants.*;

public class OrientDb implements IDatabase, Serializable {


    public static void main(String[] args) {
        OrientDb orientDb = new OrientDb("remote:localhost/newtest", "admin", "admin");
    }

    private TransactionalGraph graph;

    public OrientDb(String url, String username, String password) {
        graph = new OrientGraph(url, username, password);
    }


    @Override
    public void deleteSchemaElement(Integer schemaHash) {
        int i = 0;
        for (Vertex v : graph.getVertices(SE_ID, schemaHash)) {
            graph.removeVertex(v);
            i++;
        }
        if (i > 1) {
            System.err.println("There were multiple vertices with the same hash!!!");
        }
        graph.commit();
    }

    @Override
    public void deleteSchemaEdge(Integer linkHash) {
        for (Edge e : graph.getEdges(SL_ID, linkHash)) {
            graph.removeEdge(e);
        }
        graph.commit();
    }

    @Override
    public boolean exists(Integer schemaHash) {
        Iterator<Vertex> iterator = graph.getVertices(SE_ID, schemaHash).iterator();
        if (iterator.hasNext())
            return true;
        else
            return false;
    }

    @Override
    public void writeSchemaElementWithEdges(ISchemaElement schemaElement) {
        Vertex vertex = graph.addVertex(CLASS_SCHEMA_ELEMENT);
        vertex.setProperty(SE_ID, schemaElement.getID());

        //TODO: check if this is a good idea
        vertex.setProperty(PROPERTY_VALUES, schemaElement.getLabel());

        for (Tuple2<graph.Edge, graph.Edge> edgeTuple2 : schemaElement.getSchemaEdges()) {
            graph.Edge schemaEdge = edgeTuple2._2;
            Integer endID = schemaEdge.end == null ? EMPTY_SCHEMA_ELEMENT_HASH : Integer.valueOf(schemaEdge.end);
            Vertex targetV = getVertexByHashID(endID);
            if(targetV == null) {
                //This node does not yet exist, so create one (Hint: if it is a complex schema, you will need to add information about this one later)
                targetV = graph.addVertex(CLASS_SCHEMA_ELEMENT);
                targetV.setProperty(SE_ID, endID);
            }
            vertex.addEdge(PROPERTY_SCHEMA_RELATION, targetV);
            graph.commit(); //TODO: check if this is a good place
        }
    }

    private Vertex getVertexByHashID(Integer schemaHash){
        Iterator<Vertex> iterator = graph.getVertices(SE_ID, schemaHash).iterator();
        if (iterator.hasNext())
            return iterator.next();
        else
            return null;
    }

    @Override
    public void deletePayloadElement(Integer payloadHash) {

    }

    @Override
    public void deletePayloadEdge(Integer linkHash) {

    }

    @Override
    public void close() {
        graph.shutdown();
    }
}
