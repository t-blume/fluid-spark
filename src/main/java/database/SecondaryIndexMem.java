package database;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SecondaryIndexMem implements Serializable {


    private SecondaryIndexMem(boolean trackChanges, String indexFile) {
        schemaElementToImprint = new HashMap<>();
        storedImprints = new HashMap<>();
        this.indexFile = indexFile;
        this.trackChanges = trackChanges;
    }

    private static SecondaryIndexMem singletonInstance = null;

    public static SecondaryIndexMem getInstance() {
        return singletonInstance;
    }

    public static void init(boolean trackChanges, String indexFile, boolean loadPreviousIndex) throws IOException {
        if (!loadPreviousIndex)
            singletonInstance = new SecondaryIndexMem(trackChanges, indexFile);
        else {
            // Reading the object from a file
            GZIPInputStream gis = new GZIPInputStream(new FileInputStream(indexFile));
            ObjectInputStream in = new ObjectInputStream(gis);
            // Method for deserialization of object
            try {
                singletonInstance = (SecondaryIndexMem) in.readObject();
                singletonInstance.readSyncSchemaLinks = new Object();
                singletonInstance.writeSyncSchemaLinks = new Object();
                singletonInstance.readSyncImprint = new Object();
                singletonInstance.writeSyncImprint = new Object();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            in.close();
            gis.close();
        }

    }

    public void persist() throws IOException {
        //Saving of object in a file
        GZIPOutputStream gis = new GZIPOutputStream(new FileOutputStream(indexFile));
        ObjectOutputStream out = new ObjectOutputStream(gis);
        // Method for serialization of object
        out.writeObject(this);
        out.close();
        gis.close();
        System.out.println("Object has been serialized");
    }


    /*
        Sync objects for parallel access
     */
    private transient Object readSyncSchemaLinks = new Object();
    private transient Object writeSyncSchemaLinks = new Object();

    private transient Object readSyncImprint = new Object();
    private transient Object writeSyncImprint = new Object();


    /*
    general settings
     */
    private String indexFile;
    private boolean trackChanges;


    //schema elements and their summarized instance (IDs)
    private HashMap<Integer, Set<Integer>> schemaElementToImprint;

    //imprints stored by ID, imprints also hold links to schema elements
    private HashMap<Integer, Imprint> storedImprints;




    /**
     * Updates all Changes on removal.
     *
     * Returns true if the schema element must be deleted as well.
     *
     * @param schemaElementID
     * @param imprintID
     * @return
     */
    public boolean removeSummarizedInstance(int schemaElementID, int imprintID) {
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                Set<Integer> imprintIDs = schemaElementToImprint.get(schemaElementID);
                if (imprintIDs != null) {
                    if (imprintIDs.size() > 1) {
                        //still some instances are referenced
                        if (trackChanges) {
                            Set<String> payloadOld = getPayload(schemaElementID);
                            imprintIDs.remove(imprintID);

                            ChangeTracker.getInstance().incRemovedInstanceToSchemaLinks();
                            schemaElementToImprint.put(schemaElementID, imprintIDs);
                            Set<String> payloadNew = getPayload(schemaElementID);
                            if (payloadOld.hashCode() != payloadNew.hashCode()) {
                                ChangeTracker.getInstance().incPayloadElementsChanged();
                                ChangeTracker.getInstance().incPayloadEntriesRemoved(payloadOld.size() - payloadNew.size());
                            }
                            return false;
                        } else {
                            if (trackChanges) {
                                //schema element is removed, so is it payload
                                ChangeTracker.getInstance().incPayloadElementsChanged();
                                ChangeTracker.getInstance().incPayloadEntriesRemoved(getPayload(schemaElementID).size());
                            }
                            imprintIDs.remove(imprintID);
                            schemaElementToImprint.put(schemaElementID, imprintIDs);
                            return false;
                        }
                    } else {
                        schemaElementToImprint.remove(schemaElementID);
                        return true;
                    }
                } else
                    return true;
            }
        }
    }


    public Set<Integer> removeSchemaElement(int schemaElementID) {
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                Set<Integer> removedImprints = schemaElementToImprint.remove(schemaElementID);
                //TODO: maybe need to remove the schema element from Imprint already here?
                if (trackChanges && removedImprints != null)
                    ChangeTracker.getInstance().incRemovedInstanceToSchemaLinks(removedImprints.size());
                return removedImprints;
            }
        }
    }


    /**
     * Actually deletes imprint vertices. All other methods only update relations between
     * schema elements and imprints. Careful: changing relations can still mean that the
     * payload changes.
     *
     * Returns a list of schema elements IDs that link to schema elements with no
     * summarized instances left.
     *
     * @param imprints
     * @return
     */
    public Set<Integer> removeImprints(Set<Imprint> imprints) {
        synchronized (readSyncImprint) {
            synchronized (writeSyncImprint) {
                for (Imprint imprint : imprints) {
                    storedImprints.remove(imprint._id);
                    if (trackChanges)
                        ChangeTracker.getInstance().incPayloadElementsChanged();
                }
            }
        }
        Set<Integer> schemaElementIDsToBeRemoved = new HashSet<>();
        imprints.forEach(I -> {
            if (trackChanges) {
                Set<String> payloadOld = getPayload(I._schemaElementID);

                //this method removed the imprint id from the schema element, thus altering the payload
                if (removeSummarizedInstance(I._schemaElementID, I._id))
                    schemaElementIDsToBeRemoved.add(I._schemaElementID);

                Set<String> payloadNew = getPayload(I._schemaElementID);
                if (payloadOld.hashCode() != payloadNew.hashCode()) {
                    ChangeTracker.getInstance().incPayloadElementsChanged();
                    ChangeTracker.getInstance().incPayloadEntriesRemoved(payloadOld.size() - payloadNew.size());
                }
            } else {
                if (removeSummarizedInstance(I._schemaElementID, I._id))
                    schemaElementIDsToBeRemoved.add(I._schemaElementID);
            }
        });
        return schemaElementIDsToBeRemoved;
    }

    /**
     * This method is called when a new schema structure is freshly build.
     * Only updates relationships between imprints of instances and schema elements
     * @param schemaElementID
     * @param imprintIDs
     */
    public void putSummarizedInstances(int schemaElementID, Set<Integer> imprintIDs) {
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                schemaElementToImprint.put(schemaElementID, imprintIDs);
                if (trackChanges)
                    ChangeTracker.getInstance().incAddedInstanceToSchemaLinks(imprintIDs.size());
            }
        }
    }

    /**
     * Similar to put, but merges with existing content.
     *
     * @param schemaElementID
     * @param imprintIDs
     */
    public void addSummarizedInstances(int schemaElementID, Set<Integer> imprintIDs) {
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                if (trackChanges) {
                    Set<Integer> prev = schemaElementToImprint.get(schemaElementID);
                    int prevSize = prev == null ? 0 : prev.size();
                    prev.addAll(imprintIDs);
                    ChangeTracker.getInstance().incAddedInstanceToSchemaLinks(prev.size() - prevSize);
                    schemaElementToImprint.put(schemaElementID, prev);
                } else {
                    schemaElementToImprint.merge(schemaElementID, imprintIDs, (O, N) -> {
                        O.addAll(N);
                        return O;
                    });
                }
            }
        }
    }


//    /**
//     * counting the removed instance links only in removeSummarizedInstance()
//     *
//     * @param imprintID
//     */
//    public void removeImprintLinks(int imprintID) {
//        Imprint imprint = null;
//        synchronized (readSyncImprint) {
//            synchronized (writeSyncImprint) {
//                imprint = storedImprints.remove(imprintID);
//            }
//        }
//        if (imprint != null)
//            removeSummarizedInstance(imprint._schemaElementID, imprintID);
//
//    }

    public void addNodesToSchemaElement(Map<Integer, Set<String>> nodes, Integer schemaHash) {
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                synchronized (readSyncImprint) {
                    synchronized (writeSyncImprint) {
                        //current state
                        Set<Integer> imprintIDs = schemaElementToImprint.get(schemaHash);
                        for (Map.Entry<Integer, Set<String>> node : nodes.entrySet()) {
                            //update imprint if necessary
                            Imprint imprint = storedImprints.get(node.getKey());
                            if (imprint != null) {
                                //handle payload updates here
                                if (imprint._payload.hashCode() != node.getValue().hashCode()) {
                                    //the payload extracted from that instance has changed
                                    //set the payload exactly to the new one
                                    imprint._payload = updatePayload(imprint._payload, node.getValue(), false, false);
                                }
                                //set current time so avoid deletion after completion
                                imprint._timestamp = System.currentTimeMillis();
                                imprint._schemaElementID = schemaHash;
                            } else {
                                //create new one
                                imprint = new Imprint(node.getKey(), System.currentTimeMillis(),
                                        updatePayload(new HashSet<>(), node.getValue(), true, false),
                                        schemaHash);
                                storedImprints.put(imprint._id, imprint);
                                if (trackChanges)
                                    ChangeTracker.getInstance().incInstancesNew();
                            }

                            //add also a link from schema element to (newly) summarized instance
                            imprintIDs.add(node.getKey());
                            schemaElementToImprint.put(schemaHash, imprintIDs);
                        }
                    }
                }
            }
        }
    }


    /**
     *
     * TODO: probably does not track paylaod changes correctly.
     * updates payload and timestamp
     * @param nodes
     */
    public void touchMultiple(Map<Integer, Set<String>> nodes) {
        synchronized (readSyncImprint) {
            synchronized (writeSyncImprint) {
                for (Map.Entry<Integer, Set<String>> node : nodes.entrySet()) {
                    //update imprint if necessary
                    Imprint imprint = storedImprints.get(node.getKey());
                    if (imprint != null) {
                        //handle payload updates here
                        if (imprint._payload.hashCode() != node.getValue().hashCode()) {
                            System.out.println("Payload change on touch!");
                            //the payload extracted from that instance has changed
                            //set the payload exactly to the new one
                            imprint._payload = updatePayload(imprint._payload, node.getValue(), false, false);
                        }
                        //set current time so avoid deletion after completion
                        imprint._timestamp = System.currentTimeMillis();
                    } else {
                        System.err.println("This should not happen!");
                    }
                }
                if (trackChanges)
                    ChangeTracker.getInstance().incInstancesNotChanged(nodes.size());
            }
        }
    }


    /**
     * Streams over complete memory database and filters for imprints with an old timestamp.
     * Returns the set of schema element Ids that have to be deleted since to instance is summarized by them anymore.
     *
     * @param timestamp
     * @return
     */
    public Set<Integer> removeOldImprints(long timestamp) {
        Set<Imprint> imprintSet = storedImprints.values().parallelStream().filter(I -> I._timestamp < timestamp).collect(Collectors.toSet());
        return removeImprints(imprintSet);
    }


    //GETTER:

    public Integer getSchemaElementFromImprintID(int imprintID) {
        synchronized (readSyncImprint) {
            Imprint imprint = storedImprints.get(imprintID);
            if (imprint != null)
                return imprint._schemaElementID;
            else return null;
        }
    }


    /**
     * Read only! Returns the summarized instances from the schema element.
     * @param schemaElementID
     * @return
     */
    public Set<Imprint> getSummarizedInstances(int schemaElementID) {
        synchronized (readSyncSchemaLinks) {
            Set<Integer> imprintIDs = schemaElementToImprint.get(schemaElementID);
            if (imprintIDs != null) {
                synchronized (readSyncImprint) {
                    Set<Imprint> resultImprints = new HashSet<>();
                    for (Integer imprintID : imprintIDs) {
                        Imprint imp = storedImprints.get(imprintID);
                        if (imp != null)
                            resultImprints.add(imp);
                    }
                    return resultImprints;
                }
            } else
                return null;
        }
    }

    /**
     *
     * @param schemaHash
     * @return
     */
    public Set<String> getPayload(int schemaHash) {
        Set<Imprint> imprints = getSummarizedInstances(schemaHash);
        Set<String> payload = new HashSet<>();
        if (imprints != null) {
            for (Imprint imprint : imprints)
                if (imprint._payload != null)
                    payload.addAll(imprint._payload);
        }
        return payload;
    }



    /////////////////////////////// syntactic sugar ///////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    /**
     * Updates the schema to imprint link.
     * Convenience wrapper for removeSummarizedInstance()
     * @param imprintIDs
     */
    public void removeImprintLinksByID(Set<Integer> imprintIDs) {
        Set<Imprint> imprints = new HashSet<>();
        synchronized (readSyncImprint) {
            synchronized (writeSyncImprint) {
                for (int id : imprintIDs)
                    imprints.add(storedImprints.remove(id));
            }
        }
        imprints.forEach(I -> removeSummarizedInstance(I._schemaElementID, I._id));
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////


    private Set<String> updatePayload(Set<String> oldPayload, Set<String> newPayload, boolean addOnly, boolean removeOnly) {
        if (!trackChanges) {
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
                    ChangeTracker.getInstance().incPayloadElementsChanged();
                    ChangeTracker.getInstance().incPayloadEntriesRemoved(deletions);
                }
                return oldPayload;
            } else {
                if (addOnly) {
                    int before = oldPayload.size();
                    oldPayload.addAll(newPayload);
                    int additions = oldPayload.size() - before;
                    if (additions > 0) {
                        ChangeTracker.getInstance().incPayloadElementsChanged();
                        ChangeTracker.getInstance().incPayloadEntriesAdded(additions);
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
                        ChangeTracker.getInstance().incPayloadElementsChanged();
                        ChangeTracker.getInstance().incPayloadEntriesAdded(additions);
                        ChangeTracker.getInstance().incPayloadEntriesRemoved(deletions);
                    }
                    return newPayload;
                }
            }
        }
    }


}
