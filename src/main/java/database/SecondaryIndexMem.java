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

    public static void deactivate() {
        singletonInstance = null;
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
                singletonInstance.schemaElementsToBeRemoved = new HashSet<>();
                singletonInstance.timeTrackLock = new Object();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            in.close();
            gis.close();
        }

    }

    public long persist() throws IOException {
        //Saving of object in a file
        GZIPOutputStream gis = new GZIPOutputStream(new FileOutputStream(indexFile));
        ObjectOutputStream out = new ObjectOutputStream(gis);
        // Method for serialization of object
        out.writeObject(this);
        out.close();
        gis.close();
        System.out.println("Object has been serialized");
        System.out.println("Update time: " + timeSpentUpdating + ", reading time: " + timeSpentReading +
                ", waiting time: " + timeSpentWaiting + " (total: " + (timeSpentReading+timeSpentWaiting+timeSpentUpdating) + ")");
        return new File(indexFile).length();
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
    private transient Object timeTrackLock = new Object();
    private transient long timeSpentReading = 0L;
    private transient long timeSpentUpdating = 0L;
    private transient long timeSpentWaiting = 0L;

    public long getTimeSpentReading() {
        return timeSpentReading;
    }

    public long getTimeSpentUpdating() {
        return timeSpentUpdating;
    }

    public long getTimeSpentWaiting() {
        return timeSpentWaiting;
    }

    public long getSchemaToImprintLinks(){
        return schemaElementToImprint.values().stream().mapToLong(E -> E.size()).sum();
    }

    public int getSchemaLinks(){
        return schemaElementToImprint.size();
    }

    public int getImprintLinks(){
        return storedImprints.size();
    }

    //schema elements and their summarized instance (IDs)
    private HashMap<Integer, Set<Integer>> schemaElementToImprint;

    //imprints stored by ID, imprints also hold links to schema elements
    private HashMap<Integer, Imprint> storedImprints;

    //if schema elements should be removed first collect ids here to avoid unnecessary updates
    private transient HashSet<Integer> schemaElementsToBeRemoved = new HashSet<>();

    public HashSet<Integer> getSchemaElementsToBeRemoved() {
        return schemaElementsToBeRemoved;
    }

    /**
     * Updates all Changes on removal.
     * <p>
     * Returns true if the schema element must be deleted as well.
     *
     * @param schemaElementID
     * @param imprintID
     * @return
     */
    public boolean removeSummarizedInstance(int schemaElementID, int imprintID) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                synchronized (timeTrackLock) {
                    timeSpentWaiting += (System.currentTimeMillis() - start);
                }
                start = System.currentTimeMillis();
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
                            synchronized (timeTrackLock) {
                                timeSpentUpdating += (System.currentTimeMillis() - start);
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
                            synchronized (timeTrackLock) {
                                timeSpentUpdating += (System.currentTimeMillis() - start);
                            }
                            return false;
                        }
                    } else {
                        Set<Integer> removedImprints = schemaElementToImprint.remove(schemaElementID);
                        if (trackChanges && removedImprints != null)
                            ChangeTracker.getInstance().incRemovedInstanceToSchemaLinks(removedImprints.size());

                        //NEW, save before delete
                        schemaElementsToBeRemoved.add(schemaElementID);
                        synchronized (timeTrackLock) {
                            timeSpentUpdating += (System.currentTimeMillis() - start);
                        }
                        return true;
                    }
                } else {
                    synchronized (timeTrackLock) {
                        timeSpentUpdating += (System.currentTimeMillis() - start);
                    }
                    return true;
                }
            }
        }
    }


    public Set<Integer> removeSchemaElement(int schemaElementID) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                synchronized (timeTrackLock) {
                    timeSpentWaiting += (System.currentTimeMillis() - start);
                }
                start = System.currentTimeMillis();
                Set<Integer> removedImprints = schemaElementToImprint.remove(schemaElementID);
                if (trackChanges && removedImprints != null)
                    ChangeTracker.getInstance().incRemovedInstanceToSchemaLinks(removedImprints.size());
                synchronized (timeTrackLock) {
                    timeSpentUpdating += (System.currentTimeMillis() - start);
                }
                return removedImprints;
            }
        }
    }


    /**
     * Actually deletes imprint vertices. All other methods only update relations between
     * schema elements and imprints. Careful: changing relations can still mean that the
     * payload changes.
     * <p>
     * Returns a list of schema elements IDs that link to schema elements with no
     * summarized instances left.
     *
     * @param imprints
     * @return
     */
    public Set<Integer> removeImprints(Set<Imprint> imprints) {
        long start = System.currentTimeMillis();
        synchronized (readSyncImprint) {
            synchronized (writeSyncImprint) {
                synchronized (timeTrackLock) {
                    timeSpentWaiting += (System.currentTimeMillis() - start);
                }
                start = System.currentTimeMillis();
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
        synchronized (timeTrackLock) {
            timeSpentUpdating += (System.currentTimeMillis() - start);
        }
        return schemaElementIDsToBeRemoved;
    }

    /**
     * This method is called when a new schema structure is freshly build.
     * Only updates relationships between imprints of instances and schema elements
     *
     * @param schemaElementID
     * @param imprintIDs
     */
    public void putSummarizedInstances(int schemaElementID, Set<Integer> imprintIDs) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                synchronized (timeTrackLock) {
                    timeSpentWaiting += (System.currentTimeMillis() - start);
                }
                start = System.currentTimeMillis();
                schemaElementToImprint.put(schemaElementID, imprintIDs);
                if (trackChanges)
                    ChangeTracker.getInstance().incAddedInstanceToSchemaLinks(imprintIDs.size());
                synchronized (timeTrackLock) {
                    timeSpentUpdating += (System.currentTimeMillis() - start);
                }
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
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                synchronized (timeTrackLock) {
                    timeSpentWaiting += (System.currentTimeMillis() - start);
                }
                start = System.currentTimeMillis();
                if (trackChanges) {
                    Set<Integer> prev = schemaElementToImprint.get(schemaElementID);
                    if (prev == null)
                        prev = new HashSet<>();
                    int prevSize = prev.size();

                    prev.addAll(imprintIDs);
                    ChangeTracker.getInstance().incAddedInstanceToSchemaLinks(prev.size() - prevSize);
                    schemaElementToImprint.put(schemaElementID, prev);
                } else {
                    schemaElementToImprint.merge(schemaElementID, imprintIDs, (O, N) -> {
                        O.addAll(N);
                        return O;
                    });
                }
                synchronized (timeTrackLock) {
                    timeSpentUpdating += (System.currentTimeMillis() - start);
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
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                synchronized (readSyncImprint) {
                    synchronized (writeSyncImprint) {
                        synchronized (timeTrackLock) {
                            timeSpentWaiting += (System.currentTimeMillis() - start);
                        }
                        start = System.currentTimeMillis();
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

                            if (schemaElementsToBeRemoved.contains(schemaHash))
                                schemaElementsToBeRemoved.remove(schemaHash);
                        }
                        synchronized (timeTrackLock) {
                            timeSpentUpdating += (System.currentTimeMillis() - start);
                        }
                    }
                }
            }
        }
    }


    /**
     * TODO: probably does not track paylaod changes correctly.
     * updates payload and timestamp
     *
     * @param nodes
     */
    public void touchMultiple(Map<Integer, Set<String>> nodes) {
        long start = System.currentTimeMillis();
        synchronized (readSyncImprint) {
            synchronized (writeSyncImprint) {
                synchronized (timeTrackLock) {
                    timeSpentWaiting += (System.currentTimeMillis() - start);
                }
                start = System.currentTimeMillis();
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
                    } else {
                        System.err.println("This should not happen!");
                    }
                }
                if (trackChanges)
                    ChangeTracker.getInstance().incInstancesNotChanged(nodes.size());
                synchronized (timeTrackLock) {
                    timeSpentUpdating += (System.currentTimeMillis() - start);
                }
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
        long start = System.currentTimeMillis();
        Set<Imprint> imprintSet = storedImprints.values().parallelStream().filter(I -> I._timestamp < timestamp).collect(Collectors.toSet());
        synchronized (timeTrackLock) {
            timeSpentReading += (System.currentTimeMillis() - start);
        }
        return removeImprints(imprintSet);
    }


    //GETTER:

    public Integer getSchemaElementFromImprintID(int imprintID) {
        long start = System.currentTimeMillis();
        synchronized (readSyncImprint) {
            synchronized (timeTrackLock) {
                timeSpentWaiting += (System.currentTimeMillis() - start);
            }
            start = System.currentTimeMillis();
            Imprint imprint = storedImprints.get(imprintID);
            synchronized (timeTrackLock){
                timeSpentReading += (System.currentTimeMillis() - start);
            }
            if (imprint != null)
                return imprint._schemaElementID;
            else return null;
        }
    }


    /**
     * Read only! Returns the summarized instances from the schema element.
     *
     * @param schemaElementID
     * @return
     */
    public Set<Imprint> getSummarizedInstances(int schemaElementID) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (timeTrackLock) {
                timeSpentWaiting += (System.currentTimeMillis() - start);
            }
            start = System.currentTimeMillis();
            Set<Integer> imprintIDs = schemaElementToImprint.get(schemaElementID);
            synchronized (timeTrackLock) {
                timeSpentReading += (System.currentTimeMillis() - start);
            }
            if (imprintIDs != null) {
                start = System.currentTimeMillis();
                synchronized (readSyncImprint) {
                    synchronized (timeTrackLock) {
                        timeSpentWaiting += (System.currentTimeMillis() - start);
                    }
                    start = System.currentTimeMillis();
                    Set<Imprint> resultImprints = new HashSet<>();
                    for (Integer imprintID : imprintIDs) {
                        Imprint imp = storedImprints.get(imprintID);
                        if (imp != null)
                            resultImprints.add(imp);
                    }
                    synchronized (timeTrackLock) {
                        timeSpentReading += (System.currentTimeMillis() - start);
                    }
                    return resultImprints;
                }
            } else
                return null;
        }
    }

    /**
     * @param schemaHash
     * @return
     */
    public Set<String> getPayload(int schemaHash) {
        Set<Imprint> imprints = getSummarizedInstances(schemaHash);
        long start = System.currentTimeMillis();
        Set<String> payload = new HashSet<>();
        if (imprints != null) {
            for (Imprint imprint : imprints)
                if (imprint._payload != null)
                    payload.addAll(imprint._payload);
        }
        synchronized (timeTrackLock){
            timeSpentReading += (System.currentTimeMillis() - start);
        }
        return payload;
    }


    /////////////////////////////// syntactic sugar ///////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Updates the schema to imprint link.
     * Convenience wrapper for removeSummarizedInstance()
     *
     * @param imprintIDs
     */
    public void removeImprintLinksByID(Set<Integer> imprintIDs) {
        long start = System.currentTimeMillis();
        Set<Imprint> imprints = new HashSet<>();
        synchronized (readSyncImprint) {
            synchronized (writeSyncImprint) {
                synchronized (timeTrackLock){
                    timeSpentWaiting += (System.currentTimeMillis() - start);
                }
                start = System.currentTimeMillis();
                for (int id : imprintIDs)
                    imprints.add(storedImprints.remove(id));
                synchronized (timeTrackLock){
                    timeSpentUpdating += (System.currentTimeMillis() - start);
                }
            }
        }
        imprints.forEach(I -> removeSummarizedInstance(I._schemaElementID, I._id));
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////


    private Set<String> updatePayload(Set<String> oldPayload, Set<String> newPayload, boolean addOnly, boolean removeOnly) {
        long start = System.currentTimeMillis();
        if (!trackChanges) {
            if (addOnly) {
                oldPayload.addAll(newPayload);
                synchronized (timeTrackLock){
                    timeSpentUpdating += (System.currentTimeMillis()  - start);
                }
                return oldPayload;
            } else if (removeOnly) {
                oldPayload.removeAll(newPayload);
                synchronized (timeTrackLock){
                    timeSpentUpdating += (System.currentTimeMillis()  - start);
                }
                return oldPayload;
            } else {
                synchronized (timeTrackLock){
                    timeSpentUpdating += (System.currentTimeMillis()  - start);
                }
                return newPayload;
            }
        } else {
            if (removeOnly) {
                int before = oldPayload.size();
                oldPayload.removeAll(newPayload);
                int deletions = before - oldPayload.size();
                if (deletions > 0) {
                    ChangeTracker.getInstance().incPayloadElementsChanged();
                    ChangeTracker.getInstance().incPayloadEntriesRemoved(deletions);
                }
                synchronized (timeTrackLock){
                    timeSpentUpdating += (System.currentTimeMillis()  - start);
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
                    synchronized (timeTrackLock){
                        timeSpentUpdating += (System.currentTimeMillis()  - start);
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
                    synchronized (timeTrackLock){
                        timeSpentUpdating += (System.currentTimeMillis()  - start);
                    }
                    return newPayload;
                }
            }
        }
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        storedImprints.forEach((k, v) -> sb.append(v.toString() + "\n"));
        System.out.println("-----------------");
        schemaElementToImprint.forEach((k, v) -> sb.append("{" + k + "->" + v.toString() + "}"));
        return sb.toString();
    }

}
