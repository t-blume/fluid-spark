package database;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import scala.Serializable;
import schema.SchemaElement;
import utils.MyHash;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static database.Constants.*;

/**
 * NOTE from Tinkerpop:  Edge := outVertex ---label---> inVertex.
 */
public class OrientDbOpt implements Serializable {

    public static final int MAX_RETRIES = 5;

    /************************************************
     defined once at start up for all dbs
     ************************************************/
    public static String URL = "remote:localhost";
    public static String USERNAME = "admin";
    public static String PASSWORD = "admin";
    public static String serverUser = "root";
    public static String serverPassword = "rootpwd";
    /************************************************/

    private static OrientGraphFactory factory = null;
    private static OrientDB databaseServer = null;
    private static OrientDbOpt instance = null;

    public static OrientDbOpt getInstance(String database, boolean trackChanges) {
        if (factory == null)
            factory = new OrientGraphFactory(URL + "/" + database).setupPool(1, 10);

        if (instance == null)
            instance = new OrientDbOpt(database, trackChanges);

        return instance;
    }

    /**
     * Creates the database if not existed before.
     * Optionally, cleared the content.
     *
     * @param database
     * @param clear
     */
    public static void create(String database, boolean clear) {
        databaseServer = new OrientDB(URL, serverUser, serverPassword, OrientDBConfig.defaultConfig());
        if (databaseServer.exists(database) && clear)
            databaseServer.drop(database);

        if (!databaseServer.exists(database)) {
            databaseServer.create(database, ODatabaseType.PLOCAL);
            ODatabaseSession databaseSession = databaseServer.open(database, USERNAME, PASSWORD);

            //this is quite important to align this with the OS
            databaseSession.command("ALTER DATABASE TIMEZONE \"GMT+2\"");

            /*
                Create Schema Elements
             */
            databaseSession.createVertexClass(CLASS_SCHEMA_ELEMENT);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createIndex(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_SCHEMA_HASH);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SCHEMA_VALUES, OType.EMBEDDEDSET);
            databaseSession.getClass(CLASS_SCHEMA_ELEMENT).createProperty(PROPERTY_SUMMARIZED_INSTANCES, OType.EMBEDDEDSET);

            /*
            Create relationships between schema elements
             */
            databaseSession.createEdgeClass(CLASS_SCHEMA_RELATION);
            databaseSession.getClass(CLASS_SCHEMA_RELATION).createProperty(PROPERTY_SCHEMA_HASH, OType.INTEGER);

            /*
            Create super brain
             */
            databaseSession.createClass(CLASS_IMPRINT_VERTEX);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_IMPRINT_ID, OType.INTEGER);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createIndex(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, OClass.INDEX_TYPE.UNIQUE_HASH_INDEX, PROPERTY_IMPRINT_ID);

            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_TIMESTAMP, OType.DATETIME);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_PAYLOAD, OType.EMBEDDEDSET);
            databaseSession.getClass(CLASS_IMPRINT_VERTEX).createProperty(PROPERTY_IMPRINT_RELATION, OType.INTEGER);


            databaseSession.commit();
            databaseSession.close();
        }
    }


    /******************************************
     * Start of the OrientDB Connector object *
     ******************************************/

    //name of the database
    private String database;
    //keep track of all update operations
    public ChangeTracker _changeTracker = null;

    public OrientGraph getGraph() {
        return factory.getTx();
    }

    public OrientDbOpt(String database, boolean trackChanges) {
        this.database = database;
        if (trackChanges)
            _changeTracker = new ChangeTracker();
    }


    /**
     * Deletes the schema element and all of its edges
     *
     * @param schemaHash
     */
    public void deleteSchemaElement(Integer schemaHash) {
        OrientGraph graph = factory.getTx();
        for (Vertex v : graph.getVertices(PROPERTY_SCHEMA_HASH, schemaHash)) {
            Iterator<Edge> edgeIterator = v.getEdges(Direction.OUT, CLASS_SCHEMA_RELATION).iterator();
            while (edgeIterator.hasNext()) {
                Edge edge = edgeIterator.next();
                Vertex linkedSchemaElement = edge.getVertex(Direction.IN);
                int remainingLinks = 0;
                Iterator<Edge> links = linkedSchemaElement.getEdges(Direction.IN).iterator();
                while (remainingLinks <= 2 && links.hasNext()) {
                    links.next();
                    remainingLinks++;
                }
                if (remainingLinks < 2) {
                    //when we remove this link, the linked schema element will be an orphan so remove it as well
                    deleteSchemaElement(linkedSchemaElement.getProperty(PROPERTY_SCHEMA_HASH));
                }

            }
            graph.removeVertex(v);
            if (_changeTracker != null)
                _changeTracker._schemaElementsDeleted++;
        }
        graph.commit();
        graph.shutdown();
    }


    public boolean exists(String classString, Integer hashValue) {
        boolean exists;
        if (classString == CLASS_SCHEMA_ELEMENT) {
            exists = getVertexByHashID(PROPERTY_SCHEMA_HASH, hashValue) != null;
        } else if (classString == CLASS_IMPRINT_VERTEX) {
            exists = getVertexByHashID(PROPERTY_IMPRINT_ID, hashValue) != null;
            return exists;
        } else if (classString == CLASS_IMPRINT_RELATION) {
            exists = getEdgeByHashID(PROPERTY_IMPRINT_ID, hashValue) != null;
        } else {
            System.err.println("Invalid exists-query!");
            return false;
        }
        return exists;

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
    public void writeOrUpdateSchemaElement(SchemaElement schemaElement, Set<Integer> instances, boolean primary) {
        OrientGraph graph = factory.getTx();
        if (!exists(CLASS_SCHEMA_ELEMENT, schemaElement.getID())) {
            if (_changeTracker != null && primary)
                _changeTracker._newSchemaStructureObserved++;
            //create a new schema element
            Vertex vertex = graph.addVertex("class:" + CLASS_SCHEMA_ELEMENT);
            vertex.setProperty(PROPERTY_SCHEMA_HASH, schemaElement.getID());
            vertex.setProperty(PROPERTY_SCHEMA_VALUES, schemaElement.label());
            if (instances != null)
                vertex.setProperty(PROPERTY_SUMMARIZED_INSTANCES, instances);

            schemaElement.neighbors().forEach((K, V) -> {
                // if schema computation did not set a neighbor element, then use an empty one
                Integer endID = V == null ? EMPTY_SCHEMA_ELEMENT_HASH : V.getID();
                Vertex targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                if (targetV == null) {
                    //This node does not yet exist, so create one
                    //NOTE: neighbor elements are second-class citizens that exist as long as another schema element references them
                    //NOTE: this is a recursive step depending on chaining parameterization k
                    writeOrUpdateSchemaElement(V == null ? new SchemaElement() : V, null, false);
                }
                targetV = getVertexByHashID(PROPERTY_SCHEMA_HASH, endID);
                Edge edge = graph.addEdge("class:" + CLASS_SCHEMA_RELATION, vertex, targetV, CLASS_SCHEMA_RELATION);
                edge.setProperty(PROPERTY_SCHEMA_VALUES, K);
            });
            if (_changeTracker != null)
                _changeTracker._schemaElementsAdded++;

            graph.shutdown();
        } else {
            Iterator<Vertex> iterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaElement.getID()).iterator();
            if (iterator.hasNext()) {
                Vertex vertex = iterator.next();
                Set<Integer> summarizedInstances = vertex.getProperty(PROPERTY_SUMMARIZED_INSTANCES);
                summarizedInstances.addAll(instances);
                vertex.setProperty(PROPERTY_SUMMARIZED_INSTANCES, summarizedInstances);
                graph.commit();
            }
        }
    }

    /**
     * Return a specific vertex by property. Use only properties that are unique since only
     * the first vertex matching is returned.
     *
     * @param uniqueProperty
     * @param schemaHash
     * @return
     */
    public Vertex getVertexByHashID(String uniqueProperty, Integer schemaHash) {
        OrientGraph graph = factory.getTx();
        Iterator<Vertex> iterator = graph.getVertices(uniqueProperty, schemaHash).iterator();

        if (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            graph.shutdown();
            return vertex;
        } else
            return null;

    }

    public Edge getEdgeByHashID(String uniqueProperty, Integer schemaHash) {
        OrientGraph graph = factory.getTx();
        Iterator<Edge> iterator = graph.getEdges(uniqueProperty, schemaHash).iterator();

        if (iterator.hasNext()) {
            Edge edge = iterator.next();
            graph.shutdown();
            return edge;
        } else
            return null;
    }


    public Set<String> getPayloadOfSchemaElement(Integer schemaHash) {
        OrientGraph graph = factory.getTx();
        ODatabaseSession session = databaseServer.open(database, USERNAME, PASSWORD);

        Iterator<Vertex> iterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
        if (iterator.hasNext()) {
            Set<Integer> summarizedInstances = iterator.next().getProperty(PROPERTY_SUMMARIZED_INSTANCES);
            Set<String> payload = new HashSet<>();
            if (summarizedInstances != null) {
                for (Integer nodeID : summarizedInstances) {
                    String statement = "select * from " + CLASS_IMPRINT_VERTEX + " where " + PROPERTY_IMPRINT_ID + " == " + nodeID;
                    OResultSet rs = session.query(statement);
                    OResult row;
                    if (rs.hasNext() && (row = rs.next()).getRecord().isPresent()) {
                        ODocument imprint = (ODocument) row.getRecord().get();
                        payload.addAll(imprint.getProperty(PROPERTY_PAYLOAD));
                    }
                }
            }
            graph.shutdown();
            return payload;
        }
        graph.shutdown();
        return null;
    }


    /**
     * Closes conenction to database
     */
    public void close() {

    }


    /**
     * Get linked schema element hash from instance ID
     *
     * @param nodeID
     * @return
     */
    public Integer getPreviousElementID(Integer nodeID) {
        ODatabaseSession session = databaseServer.open(database, USERNAME, PASSWORD);

        String statement = "select * from " + CLASS_IMPRINT_VERTEX + " where " + PROPERTY_IMPRINT_ID + " == " + nodeID;

        OResultSet rs = session.query(statement);
        OResult row;
        if (rs.hasNext() && (row = rs.next()).getRecord().isPresent()) {
            ODocument imprint = (ODocument) row.getRecord().get();
            return imprint.getProperty(PROPERTY_IMPRINT_RELATION);
        }else
            return null;

    }

    /**
     * Get linked schema element from instance ID
     *
     * @param nodeID
     * @return
     */
    public Vertex getPreviousElement(Integer nodeID) {
        OrientGraph graph = factory.getTx();
        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        if (iterator.hasNext()) {
            Iterator<Edge> innerIterator = iterator.next().getEdges(Direction.OUT, CLASS_IMPRINT_RELATION).iterator();
            if (innerIterator.hasNext()) {
                graph.shutdown();
                return innerIterator.next().getVertex(Direction.IN);
            }
        }
        return null;
    }

    /**
     * Returns true if it also removed the schema element
     *
     * @param nodeID
     * @param schemaHash
     * @return
     */
    public boolean removeNodeFromSchemaElement(Integer nodeID, Integer schemaHash) {
        OrientGraph graph = factory.getTx();
        //removes all edges between the imprint vertex and the schema vertex
        Iterator<Edge> edgeIterator = graph.getEdges(CLASS_IMPRINT_RELATION + "." + PROPERTY_IMPRINT_ID, MyHash.md5HashImprintRelation(nodeID, schemaHash)).iterator();
        while (edgeIterator.hasNext()) {
            graph.removeEdge(edgeIterator.next());
            if (_changeTracker != null)
                _changeTracker._removedInstanceToSchemaLinks++;
        }

        Iterator<Vertex> vertexIterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
        boolean removedSE = false;
        while (vertexIterator.hasNext()) {
            Iterator<Edge> innerIterator = vertexIterator.next().getEdges(Direction.IN, CLASS_IMPRINT_RELATION).iterator();
            if (!innerIterator.hasNext()) {
                deleteSchemaElement(schemaHash);
                removedSE = true;
                //no more instance with that schema exists
                if (_changeTracker != null)
                    _changeTracker._schemaStructureDeleted++;
            }
        }
        graph.commit();
        graph.shutdown();
        return removedSE;
    }

    public void removeNodesFromSchemaElement(Map<Integer, Integer> nodes) {

//        ODatabaseSession session = databaseServer.open(database, USERNAME, PASSWORD);
//
//        for (Map.Entry<Integer, Integer> node : nodes.entrySet()) {
//            String statement = "select * from " + CLASS_IMPRINT_VERTEX + " where " + PROPERTY_IMPRINT_ID + " == " + node.getKey();
////            System.out.println(statement);
//            OResultSet rs = session.query(statement);
//            OResult row;
//            if (rs.hasNext() && (row = rs.next()).getRecord().isPresent()) {
//                ODocument imprint = (ODocument) row.getRecord().get();
//            }}
//
        OrientGraph graph = factory.getTx();
        for (Map.Entry<Integer, Integer> node : nodes.entrySet()) {
            Iterator<Vertex> iterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, node.getValue()).iterator();
            while (iterator.hasNext()) {
                Vertex schemaElement = iterator.next();
                Set<Integer> instances = schemaElement.getProperty(PROPERTY_SUMMARIZED_INSTANCES);
                instances.remove(node.getKey());
                if (_changeTracker != null)
                    _changeTracker._removedInstanceToSchemaLinks++;
                if (instances.size() == 0) {
                    deleteSchemaElement(node.getValue());
                    //no more instance with that schema exists
                    if (_changeTracker != null)
                        _changeTracker._schemaStructureDeleted++;
                } else {
                    schemaElement.setProperty(PROPERTY_SUMMARIZED_INSTANCES, instances);
                }
                graph.commit();
            }


            Iterator<Vertex> vertexIterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, node.getValue()).iterator();
            boolean removedSE = false;
            while (vertexIterator.hasNext()) {
                Iterator<Edge> innerIterator = vertexIterator.next().getEdges(Direction.IN, CLASS_IMPRINT_RELATION).iterator();
                if (!innerIterator.hasNext()) {
                    deleteSchemaElement(node.getValue());
                    removedSE = true;
                    //no more instance with that schema exists
                    if (_changeTracker != null)
                        _changeTracker._schemaStructureDeleted++;
                }
            }
        }
        graph.commit();
        graph.shutdown();
    }

    public void addNodesToSchemaElement(Map<Integer, Set<String>> nodes, Integer schemaHash) {
        ODatabaseSession session = databaseServer.open(database, USERNAME, PASSWORD);

        for (Map.Entry<Integer, Set<String>> node : nodes.entrySet()) {
            String statement = "select * from " + CLASS_IMPRINT_VERTEX + " where " + PROPERTY_IMPRINT_ID + " == " + node.getKey();
//            System.out.println(statement);
            OResultSet rs = session.query(statement);
            OResult row;
            if (rs.hasNext() && (row = rs.next()).getRecord().isPresent()) {
                ODocument imprint = (ODocument) row.getRecord().get();
                Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
                //handle payload updates here
                if (oldPayload.hashCode() != node.getValue().hashCode()) {
                    //the payload extracted from that instance has changed
                    //set the payload to to exactly new new one
                    imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(oldPayload, node.getValue(), false, false));
                }
                //set current time so avoid deletion after completion
                imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
                imprint.setProperty(PROPERTY_IMPRINT_RELATION, schemaHash);

                imprint.save();
            } else {
                ODocument imprint = new ODocument(CLASS_IMPRINT_VERTEX);
                //unique reference to instance
                imprint.setProperty(PROPERTY_IMPRINT_ID, node.getKey());
                //memorize which payload was extracted from this instance
                imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(new HashSet<>(), node.getValue(), true, false));
                //set current time so avoid deletion after completion
                imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
                imprint.setProperty(PROPERTY_IMPRINT_RELATION, schemaHash);
                imprint.save();
            }
        }
    }

    /**
     * Update or create an imprint vertex and link it to the schema element and the payload
     *
     * @param nodeID
     * @param schemaHash
     * @param payload
     * @return
     */
    public void addNodeToSchemaElement(Integer nodeID, Integer schemaHash, Set<String> payload) {
        OrientGraphNoTx graph = factory.getNoTx();
        // get or create the imprint vertex
        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        Vertex imprint;
        if (!iterator.hasNext()) {
            imprint = graph.addVertex("class:" + CLASS_IMPRINT_VERTEX);
            //unique reference to instance
            imprint.setProperty(PROPERTY_IMPRINT_ID, nodeID);
            //memorize which payload was extracted from this instance
            imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(new HashSet<>(), payload, true, false));
            //set current time so avoid deletion after completion
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        } else {
            imprint = iterator.next();
            Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
            //handle payload updates here
            if (oldPayload.hashCode() != payload.hashCode()) {
                //the payload extracted from that instance has changed
                //set the payload to to exactly new new one
                imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(oldPayload, payload, false, false));
                //graph.commit();
            }
            //set current time so avoid deletion after completion
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
        }

        //get the schema element
        iterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
        Vertex schema = iterator.next();

        if (!exists(CLASS_IMPRINT_RELATION, MyHash.md5HashImprintRelation(nodeID, schemaHash))) {
            Edge imprintEdge = graph.addEdge("class:" + CLASS_IMPRINT_RELATION, imprint, schema, CLASS_IMPRINT_RELATION);
            imprintEdge.setProperty(PROPERTY_IMPRINT_ID, MyHash.md5HashImprintRelation(nodeID, schemaHash));
        }
        graph.commit();
        graph.shutdown();
    }


    public void touch(Integer nodeID, Set<String> payload) {
        OrientGraph graph = factory.getTx();

        // get the imprint vertex
        Iterator<Vertex> iterator = graph.getVertices(CLASS_IMPRINT_VERTEX + "." + PROPERTY_IMPRINT_ID, nodeID).iterator();
        Vertex imprint;
        if (!iterator.hasNext()) {
            System.err.println("This should not happen!");
        } else {
            imprint = iterator.next();
            imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
            Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
            if (oldPayload.hashCode() != payload.hashCode()) {
                imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(oldPayload, payload, false, false));
            }
        }
        graph.commit();
        graph.shutdown();
    }

    public void touchMultiple(Map<Integer, Set<String>> nodes) {
        ODatabaseSession session = databaseServer.open(database, USERNAME, PASSWORD);

        for (Map.Entry<Integer, Set<String>> node : nodes.entrySet()) {
            String statement = "select * from " + CLASS_IMPRINT_VERTEX + " where " + PROPERTY_IMPRINT_ID + " == ?";
            OResultSet rs = session.query(statement, node.getKey());
            OResult row = rs.next();
            if (row.getRecord().isPresent()) {
                ODocument imprint = (ODocument) row.getRecord().get();
                imprint.setProperty(PROPERTY_TIMESTAMP, NOW());
                Set<String> oldPayload = imprint.getProperty(PROPERTY_PAYLOAD);
                if (oldPayload.hashCode() != node.getValue().hashCode()) {
                    imprint.setProperty(PROPERTY_PAYLOAD, updatePayload(oldPayload, node.getValue(), false, false));
                }
                imprint.save();
            } else {
                System.err.println("This should not happen!");
            }
        }
    }

    /**
     * remove all imprints that have not been touched since defined time interval.
     * returns the number of deleted imprints
     *
     * @return
     */
    public int removeOldImprintsAndElements(String timestamp) {
        OrientGraph graph = factory.getTx();
        ODatabaseSession session = databaseServer.open(database, USERNAME, PASSWORD);
        String statement = "select * from ImprintVertex  where timestamp < ?";
        OResultSet rs = session.query(statement, timestamp);
        int i = 0;
        while (rs.hasNext()) {
            OResult row = rs.next();
            for (int retry = 0; retry < MAX_RETRIES; ++retry) {
                try {
                    int nodeID = row.getProperty(PROPERTY_IMPRINT_ID);
                    Set<String> instancePayload = row.getProperty(PROPERTY_PAYLOAD);
                    if (_changeTracker != null) {
                        _changeTracker._payloadElementsChanged++;
                        _changeTracker._payloadEntriesRemoved += instancePayload.size();
                    }

                    Integer schemaHash = row.getProperty(PROPERTY_IMPRINT_RELATION);

                    ODocument imprint = (ODocument) row.getRecord().get();
                    imprint.delete();
                    if (_changeTracker != null) {
                        _changeTracker._instancesDeleted++;
                    }

                    //iterate through all linked schema elements and check if there is still an instance linked to it
                    Iterator<Vertex> iterator = graph.getVertices(CLASS_SCHEMA_ELEMENT + "." + PROPERTY_SCHEMA_HASH, schemaHash).iterator();
                    while (iterator.hasNext()) {
                        Vertex schemaElement = iterator.next();
                        Set<Integer> instances = schemaElement.getProperty(PROPERTY_SUMMARIZED_INSTANCES);
                        instances.remove(nodeID);
                        if (_changeTracker != null)
                            _changeTracker._removedInstanceToSchemaLinks++;
                        if (instances.size() == 0) {
                            deleteSchemaElement(schemaHash);
                            //no more instance with that schema exists
                            if (_changeTracker != null)
                                _changeTracker._schemaStructureDeleted++;
                            try {
                                graph.removeVertex(schemaElement);
                                if (_changeTracker != null)
                                    _changeTracker._schemaElementsDeleted++;
                            } catch (ORecordNotFoundException e) {
                                //should be no problem since its already removed
                            }
                        } else {
                            schemaElement.setProperty(PROPERTY_SUMMARIZED_INSTANCES, instances);
                        }
                        graph.commit();
                    }

                    System.out.println(row);
                    i++;
                    break;
                } catch (OConcurrentModificationException e) {
                    System.out.println("Retry " + retry);
                }
            }
        }
        graph.shutdown();
        rs.close();
        return i;
    }


    private Set<String> updatePayload(Set<String> oldPayload, Set<String> newPayload, boolean addOnly, boolean removeOnly) {
        if (_changeTracker == null) {
            if (addOnly) {
                oldPayload.addAll(newPayload);
                return oldPayload;
            } else if (removeOnly) {
                oldPayload.removeAll(newPayload);
                return oldPayload;
            } else
                return newPayload;
        } else {
            if (removeOnly) {
                int before = oldPayload.size();
                oldPayload.removeAll(newPayload);
                int deletions = before - oldPayload.size();
                if (deletions > 0) {
                    _changeTracker._payloadElementsChanged++;
                    _changeTracker._payloadEntriesRemoved += deletions;
                }
                return oldPayload;
            } else {
                if (addOnly) {
                    int before = oldPayload.size();
                    oldPayload.addAll(newPayload);
                    int additions = oldPayload.size() - before;
                    if (additions > 0) {
                        _changeTracker._payloadElementsChanged++;
                        _changeTracker._payloadEntriesAdded += additions;
                    }
                    return oldPayload;
                } else {
                    int deletions = 0;
                    int additions = 0;
                    //pair-wise comparison needed
                    for (String oPay : oldPayload)
                        if (!newPayload.contains(oPay))
                            deletions++;

                    for (String nPay : newPayload)
                        if (!oldPayload.contains(nPay))
                            additions++;

                    if (additions > 0 || deletions > 0) {
                        _changeTracker._payloadElementsChanged++;
                        _changeTracker._payloadEntriesAdded += additions;
                        _changeTracker._payloadEntriesRemoved += deletions;
                    }
                    return newPayload;
                }
            }
        }
    }
}
