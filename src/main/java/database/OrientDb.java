package database;

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import schema.SchemaElement;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import scala.Serializable;
import scala.Tuple2;
import schema.ISchemaElement;
import utils.MyHash;

import java.util.*;

import static database.Constants.*;

public class OrientDb implements Serializable {
    public static final int MAX_RETRIES = 5;
    public static String URL = "remote:localhost";
    public static String USERNAME = "admin";
    public static String PASSWORD = "admin";
    public static String serverUser = "root";
    public static String serverPassword = "rootpwd";


    private static Map<String,OrientGraphFactory> factory = new HashMap<>();


    public static OrientDb getInstance(String database) {
        if (!factory.containsKey(database))
            factory.put(database, new OrientGraphFactory(URL + "/" + database).setupPool(1, 1));

        return new OrientDb(factory.get(database).getTx(), database);
    }

    private static OrientDB databaseServer = null;
    private static ODatabasePool pool = null;

    public static ODatabaseSession getDBSession(String database) {
        if (databaseServer == null) {
            databaseServer = new OrientDB(URL, "root", "rootpwd", OrientDBConfig.defaultConfig());
        }
        if (pool == null) {
            pool = new ODatabasePool(databaseServer, database, "admin", "admin");
        }
        // OPEN DATABASE
        try (ODatabaseSession db = pool.acquire()) {
            return db;
        }
    }

    public static void create(String database, boolean clear) {


        databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
        if (databaseServer.exists(database) && clear)
            databaseServer.drop(database);

        if (!databaseServer.exists(database)) {
            databaseServer.create(database, ODatabaseType.PLOCAL);

            ODatabaseSession databaseSession = databaseServer.open(database, USERNAME, PASSWORD);

            //this is quite important to align this with the OS
            databaseSession.command("ALTER DATABASE TIMEZONE \"GMT+2\"");

            databaseSession.createVertexClass(CLASS_SCHEMA_ELEMENT);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createIndex(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_SCHEMA_HASH);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_VALUES, OType.EMBEDDEDSET);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);

            databaseSession.createEdgeClass(CLASS_SCHEMA_RELATION);
            databaseSession.getClass(CLASS_SCHEMA_RELATION).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);
//            databaseSession.getClass(CLASS_SCHEMA_RELATION).createIndex(CLASS_SCHEMA_RELATION + "." + PROPERTY_SCHEMA_HASH, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_SCHEMA_HASH);

            databaseSession.createVertexClass(CLASS_IMPRINT_VERTEX);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_IMPRINT_ID, OType.INTEGER);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createIndex(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_IMPRINT_ID);

            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_TIMESTAMP, OType.DATETIME);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);


//            databaseSession.createVertexClass(CLASS_IMPRINT_EDGE);
//            databaseSession.getClass(CLASS_IMPRINT_EDGE).createProperty(PROPERTY_IMPRINT_ID, OType.INTEGER);
////            databaseSession.getClass(CLASS_IMPRINT_EDGE).createIndex(CLASS_IMPRINT_EDGE + "." + PROPERTY_IMPRINT_ID, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_IMPRINT_ID);
//            databaseSession.getClass(CLASS_IMPRINT_EDGE).createProperty(CLASS_IMPRINT_RELATION, OType.EMBEDDEDSET);

            databaseSession.createEdgeClass(CLASS_IMPRINT_RELATION);


            databaseSession.commit();
            databaseSession.close();

        }
    }


    public TransactionalGraph graph;
    private String database;

    public OrientDb(OrientGraph graph, String database) {
        this.graph = graph;
        this.database = database;
    }


    /**
     * Deletes the schema element and all of its edges
     *
     * @param schemaHash
     */
    public void deleteSchemaElement(Integer schemaHash) {
        for (Vertex v : graph.getVertices(PROPERTY_SCHEMA_HASH, schemaHash))
            graph.removeVertex(v);

        graph.commit();
    }


    public boolean exists(String classString, Integer schemaHash) {
        if (classString == CLASS_SCHEMA_ELEMENT) {
            boolean exists = getVertexByHashID(PROPERTY_SCHEMA_HASH, schemaHash) != null;
            return exists;
        } else if (classString == CLASS_IMPRINT_VERTEX) {
            boolean exists = getVertexByHashID(PROPERTY_IMPRINT_ID, schemaHash) != null;
            return exists;
        } else {
            System.err.println("Invalid exists query!");
            return false;
        }
    }


    /**
     * @param schemaElement
     * @deprecated
     */
    public void writeSchemaElementWithEdges(ISchemaElement schemaElement) {
        Vertex vertex = graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT);
        vertex.setProperty(PROPERTY_SCHEMA_HASH, schemaElement.getID());
        vertex.setProperty(PROPERTY_SCHEMA_VALUES, schemaElement.getLabel());

        // iterate through all schema edges
        for (Tuple2<graph.Edge, graph.Edge> edgeTuple2 : schemaElement.getSchemaEdges()) {
            //re-naming for convenience TODO: remove for performance
            graph.Edge schemaEdge = edgeTuple2._2;


            //determine the ID of the next target schema Vertex
            Integer endID = schemaEdge.end == null ? EMPTY_SCHEMA_ELEMENT_HASH : Integer.valueOf(schemaEdge.end);
            Vertex targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
            if (targetV == null) {
                //This node does not yet exist, so create one (Hint: if it is a complex schema, you will need to add information about this one later)
                targetV = graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT);
                targetV.setProperty(PROPERTY_SCHEMA_HASH, endID);
            }
            Edge edge = graph.addEdge(schemaEdge.hashCode(), vertex, targetV, CLASS_SCHEMA_RELATION);
            edge.setProperty(PROPERTY_SCHEMA_HASH, schemaEdge.hashCode());
            edge.setProperty(PROPERTY_SCHEMA_VALUES, schemaEdge.label);
            graph.commit();
        }
    }

    /**
     * CONVENTION:
     * the schema computation can return the following:
     * - no schema edges at all (k=0) => leads to no outgoing edges
     * - schema edges where we ignore the target (null) => leads to outgoing edges with an EMPTY TARGET placeholder
     * - schema edges where we take the schema of the neighbour into account
     * => write second-class schema element with no imprint vertex but shared among all first-class elements
     *
     * @param schemaElement
     */
    public void writeSchemaElementWithEdges(SchemaElement schemaElement) {
        if (!exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID())) {
            Vertex vertex = graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT);
            vertex.setProperty(PROPERTY_SCHEMA_HASH, schemaElement.getID());
            vertex.setProperty(PROPERTY_SCHEMA_VALUES, schemaElement.label());

            schemaElement.neighbors().forEach((K, V) -> {
                // if schema computation did not set a neighbor element, then use dummy one
                Integer endID = V == null ? EMPTY_SCHEMA_ELEMENT_HASH : V.getID();
                Vertex targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                if (targetV == null) {
                    //This node does not yet exist, so create one
                    //NOTE: neighbor elements are second-class citizens that exist as long as another schema element references them
                    //NOTE: this is a recursive step depending on chaining parameterization k
                    writeSchemaElementWithEdges(V == null ? new SchemaElement() : V);
                }
                targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                Edge edge = graph.addEdge("class:" + CLASS_SCHEMA_RELATION, vertex, targetV, CLASS_SCHEMA_RELATION);
                edge.setProperty(PROPERTY_SCHEMA_VALUES, K);
            });
            ChangeTracker.getSchemaElementsAddedThisIteration().add(schemaElement.getID());

            //payload
            if (schemaElement.payload().size() > 0) {
                vertex.setProperty(PROPERTY_PAYLOAD, schemaElement.payload());
                ChangeTracker.incPayloadElementsChangedThisIteration();
                ChangeTracker.incPayloadEntriesAdded(schemaElement.payload().size());
                graph.commit();
            }

        } else {
            //only update payload
            Vertex vertex = getVertexByHashID(PROPERTY_SCHEMA_HASH, schemaElement.getID());
            Set<String> payload = vertex.getProperty(PROPERTY_PAYLOAD);
            int changes = 0;
            for (String pay : schemaElement.payload()) {
                if (!payload.contains(pay)) {
                    payload.add(pay);
                    changes++;
                }
            }
            if (changes > 0) {
                vertex.setProperty(PROPERTY_PAYLOAD, payload);
                ChangeTracker.incPayloadElementsChangedThisIteration();
                ChangeTracker.incPayloadEntriesAdded(changes);
                graph.commit();
            }
        }
    }

    public Vertex getVertexByHashID(String uniqueProperty, Integer schemaHash) {
        Iterator<Vertex> iterator = graph.getVertices(uniqueProperty, schemaHash).iterator();
        if (iterator.hasNext())
            return iterator.next();
        else
            return null;
    }



    public void close() {
        graph.shutdown();
    }


    /**
     * get linked schema element hash from instance
     * @param nodeID
     * @return
     */
    public Integer getPreviousElementID(Integer nodeID) {
        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        if (iterator.hasNext()) {
            Iterator<Edge> innerIterator = iterator.next().getEdges(Direction.OUT, CLASS_IMPRINT_RELATION).iterator();
            if (innerIterator.hasNext())
                return innerIterator.next().getVertex(Direction.IN).getProperty(PROPERTY_SCHEMA_HASH);
        }

        return null;
    }

    public Vertex getPreviousElement(Integer nodeID) {
        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        if (iterator.hasNext()) {
            Iterator<Edge> innerIterator = iterator.next().getEdges(Direction.OUT, CLASS_IMPRINT_RELATION).iterator();
            if (innerIterator.hasNext())
                return innerIterator.next().getVertex(Direction.IN);
        }

        return null;
    }

    public Integer removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash) {
        StringBuilder sb = new StringBuilder();
        sb.append(nodeID);
        sb.append(schemaHash);

        //removes all edges between the imprint vertex and the schema vertex
        Iterator<Edge> iterator = graph.getEdges(CLASS_IMPRINT_RELATION + "." + PROPERTY_IMPRINT_ID, sb.toString().hashCode()).iterator();
        while (iterator.hasNext())
            graph.removeEdge(iterator.next());
        //TODO: remove schema element here and maybe the imprint vertex

        graph.commit();
        return 1;
    }


    /**
     * Update or create an imprint vertex and link it to the schema element and the payload
     * @param nodeID
     * @param schemaHash
     * @param payload
     * @return
     */
    public void addNodeToSchemaElement(Integer nodeID, Integer schemaHash, Set<String> payload) {
        // get or create the imprint vertex
        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        Vertex imprint;
        if (!iterator.hasNext()) {
            imprint = graph.addVertex("class:" + CLASS_IMPRINT_VERTEX);
            //unique reference to instance
            imprint.setProperty(PROPERTY_IMPRINT_ID, nodeID);
            //memorize which payload was extracted from this instance TODO: data overhead could be reduced by not storing payload in SE
            imprint.setProperty(PROPERTY_PAYLOAD, payload);
            //set current time so avoid deletion after completion
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        } else {
            imprint = iterator.next();
            //TODO: handle updates here
            imprint.setProperty(PROPERTY_PAYLOAD, payload);
            //set current time so avoid deletion after completion
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        }


        iterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
        Vertex schema = iterator.next();

        StringBuilder sb = new StringBuilder();
        sb.append(nodeID);
        sb.append(schemaHash);
        int edgeID = MyHash.md5HashString(sb.toString());


        Edge imprintEdge = graph.addEdge(edgeID, imprint, schema, CLASS_IMPRINT_RELATION);
        imprintEdge.setProperty(PROPERTY_IMPRINT_ID, edgeID);
        graph.commit();

    }


    public void touch(Integer nodeID) {
        // get the imprint vertex
        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        Vertex imprint;
        if (!iterator.hasNext()) {
            imprint = graph.addVertex("class:" + CLASS_IMPRINT_VERTEX);
            imprint.setProperty(PROPERTY_IMPRINT_ID, nodeID);
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        } else {
            imprint = iterator.next();
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        }
        graph.commit();
    }


    public Integer getPreviousLinkID(Integer edgeID) {
        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_EDGE + "." + PROPERTY_IMPRINT_ID, edgeID).iterator();
        if (iterator.hasNext())
            return iterator.next().getProperty(PROPERTY_SCHEMA_HASH);
        return null;
    }


    public Integer removeEdgeFromSchemaEdge(Integer edgeID, Integer linkHash) {
        StringBuilder sb = new StringBuilder();
        sb.append(edgeID);
        sb.append(linkHash);

        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_EDGE + "." + PROPERTY_IMPRINT_ID, sb.toString().hashCode()).iterator();
        int size = 0;
        while (iterator.hasNext()) {
            Vertex imprint = iterator.next();
            Set<Integer> links = imprint.getProperty(CLASS_IMPRINT_RELATION);
            links.remove(linkHash);
            size = links.size();
            if (size <= 0)
                graph.removeVertex(imprint);
            else
                imprint.setProperty(CLASS_IMPRINT_RELATION, links);
        }
        graph.commit();
        return size;
    }


    public Integer addEdgeFromSchemaEdge(Integer edgeID, Integer linkHash) {
        StringBuilder sb = new StringBuilder();
        sb.append(edgeID);
        sb.append(linkHash);

        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_EDGE + "." + PROPERTY_IMPRINT_ID, edgeID).iterator();

        Vertex imprint;
        if (!iterator.hasNext()) {
            imprint = graph.addVertex("class:" + CLASS_IMPRINT_EDGE);
            imprint.setProperty(PROPERTY_IMPRINT_ID, edgeID);
            imprint.setProperty(CLASS_IMPRINT_RELATION, new HashSet<>());
        } else
            imprint = iterator.next();


        Set<Integer> links = imprint.getProperty(CLASS_IMPRINT_RELATION);
        links.add(linkHash);
        imprint.setProperty(CLASS_IMPRINT_RELATION, links);
        int size = links.size();

        graph.commit();
        return size;
    }

    /**
     * remove all imprints that have not been touched since defined time interval.
     * returns the number of deleted imprints
     *
     * @return
     */
    public int removeOldImprintsAndElements(String timestamp) {
        ODatabaseSession session = databaseServer.open(database, USERNAME, PASSWORD);
        String statement = "select * from ImprintVertex  where timestamp < ?";
        OResultSet rs = session.query(statement, timestamp);
        int i = 0;
        while (rs.hasNext()) {
            for (int retry = 0; retry < MAX_RETRIES; ++retry) {
                try {
                    OResult row = rs.next();
                    int nodeID = row.getProperty(PROPERTY_IMPRINT_ID);
                    Set<String> instancePayload = row.getProperty(PROPERTY_PAYLOAD);
                    Set<Vertex> linkedSchemaElements = new HashSet<>();
                    for (Vertex v : graph.getVertices(PROPERTY_IMPRINT_ID, nodeID)) {
                        Iterator<Edge> edgeIterator = v.getEdges(Direction.OUT).iterator();
                        while (edgeIterator.hasNext()) {
                            Edge edge = edgeIterator.next();
                            Vertex linkedSchemaElement = edge.getVertex(Direction.OUT);
                            //delete payload from this element
                            instancePayload.forEach(IP -> {
                                System.out.println("Checking to remove payload: " + IP);
                                if(((Set<String>) linkedSchemaElement.getProperty(PROPERTY_PAYLOAD)).contains(IP)){
                                    Set<String> payload =  linkedSchemaElement.getProperty(PROPERTY_PAYLOAD);
                                    ChangeTracker.incPayloadEntriesRemoved(1);
                                    payload.remove(IP);
                                    System.out.println("removed payload " + IP + " from " + linkedSchemaElement.getProperty(PROPERTY_SCHEMA_HASH));
                                    linkedSchemaElement.setProperty(PROPERTY_PAYLOAD, payload);
                                    graph.commit();
                                }
                            });
                            linkedSchemaElements.add(linkedSchemaElement);
                        }
                        graph.removeVertex(v);
                    }
                    graph.commit();
                    //iterate through all linked schema elements and check if there is still an instance linked to it
                    for (Vertex linkedSchemaElement : linkedSchemaElements) {
                        if (!linkedSchemaElement.getEdges(Direction.IN, CLASS_IMPRINT_RELATION).iterator().hasNext()) {
                            try {
                                graph.removeVertex(linkedSchemaElement);
                                ChangeTracker.getSchemaElementsDeletedThisIteration().add(linkedSchemaElement.getProperty(PROPERTY_SCHEMA_HASH));
                                graph.commit();
                            }catch (ORecordNotFoundException e){
                                //should be no problem since its already removed
                            }
                        }
                    }

                    ChangeTracker.incRemovedInstancesSchemaLinks();
                    graph.commit();
                    System.out.println(row);
                    i++;
                    break;
                } catch (OConcurrentModificationException e) {
                    System.out.println("Retry " + retry);
                }
            }

        }

        rs.close();
        return i;
    }
}
