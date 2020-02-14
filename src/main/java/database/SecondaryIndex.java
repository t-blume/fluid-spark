package database;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SecondaryIndex implements Serializable {
    private static final Logger logger = LogManager.getLogger(SecondaryIndex.class.getSimpleName());

    private static final boolean TRACK_PAYLOAD_DETAILS = true;

    private SecondaryIndex(boolean trackAllChanges, boolean trackMandatory, boolean trackExecutionTimes, String indexFile) {
        schemaElementToImprint = new HashMap<>();
        storedImprints = new HashMap<>();
        this.indexFile = indexFile;
        this.trackAllChanges = trackAllChanges;
        this.trackMandatory = trackMandatory;
        this.trackExecutionTimes = trackExecutionTimes;
    }

//    private static SecondaryIndex singletonInstance = null;
//
//    public static SecondaryIndex getInstance() {
//        return singletonInstance;
//    }

//    public static void deactivate() {
//        singletonInstance = null;
//    }

//    public static void init(boolean trackAllChanges, boolean trackMandatory, boolean trackExecutionTimes, String indexFile, boolean loadPreviousIndex) throws IOException {
//        if (!loadPreviousIndex)
//            singletonInstance = new SecondaryIndex(trackAllChanges, trackMandatory, trackExecutionTimes, indexFile);
//        else {
//            // Reading the object from a file
//            GZIPInputStream gis = new GZIPInputStream(new FileInputStream(indexFile));
//            ObjectInputStream in = new ObjectInputStream(gis);
//            // Method for deserialization of object
//            try {
//                singletonInstance = (SecondaryIndex) in.readObject();
//                singletonInstance.readSyncSchemaLinks = new Object();
//                singletonInstance.writeSyncSchemaLinks = new Object();
//                singletonInstance.readSyncImprint = new Object();
//                singletonInstance.writeSyncImprint = new Object();
//                singletonInstance.schemaElementsToBeRemoved = new HashSet<>();
//            } catch (ClassNotFoundException e) {
//                e.printStackTrace();
//            }
//            in.close();
//            gis.close();
//        }
//    }

    public static SecondaryIndex instantiate(boolean trackAllChanges, boolean trackMandatory, boolean trackExecutionTimes, String indexFile, boolean loadPreviousIndex) throws IOException {
        SecondaryIndex secondaryIndex = null;
        if (!loadPreviousIndex)
            secondaryIndex = new SecondaryIndex(trackAllChanges, trackMandatory, trackExecutionTimes, indexFile);
        else {
            // Reading the object from a file
            GZIPInputStream gis = new GZIPInputStream(new FileInputStream(indexFile));
            ObjectInputStream in = new ObjectInputStream(gis);
            // Method for deserialization of object
            try {
                secondaryIndex = (SecondaryIndex) in.readObject();
                secondaryIndex.readSyncSchemaLinks = new Object();
                secondaryIndex.writeSyncSchemaLinks = new Object();
                secondaryIndex.readSyncImprint = new Object();
                secondaryIndex.writeSyncImprint = new Object();
                secondaryIndex.schemaElementsToBeRemoved = new HashSet<>();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            in.close();
            gis.close();
        }
        return secondaryIndex;
    }

    public long persist() throws IOException {
        //Saving of object in a file
        GZIPOutputStream gis = new GZIPOutputStream(new FileOutputStream(indexFile));
        ObjectOutputStream out = new ObjectOutputStream(gis);
        // Method for serialization of object
        out.writeObject(this);
        out.close();
        gis.close();
        logger.info(this.getClass().getSimpleName() + " has been serialized.");
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
    private final String indexFile;
    private final boolean trackAllChanges;
    private final boolean trackMandatory;
    private final boolean trackExecutionTimes;

    //schema elements and their summarized instance (IDs)
    private HashMap<Integer, Set<Integer>> schemaElementToImprint;

    //imprints stored by ID, imprints also hold links to schema elements
    private HashMap<Integer, Imprint> storedImprints;

    private HashMap<Integer, Set<Integer>> schemaRelationToEdgeImprints;



    //if schema elements should be removed first collect ids here to avoid unnecessary updates (
    private transient HashSet<Integer> schemaElementsToBeRemoved = new HashSet<>();

    public HashSet<Integer> getSchemaElementsToBeRemoved() {
        return schemaElementsToBeRemoved;
    }

    public void addPayload(int imprintID, Set<String> payload){
        //TODO: count payload changes
        synchronized (readSyncImprint){
            synchronized (writeSyncImprint){
                storedImprints.get(imprintID)._payload.addAll(payload);
            }
        }
    }

    public void removePayload(int imprintID, Set<String> payload){
        //TODO: count payload changes
        synchronized (readSyncImprint){
            synchronized (writeSyncImprint){
                storedImprints.get(imprintID)._payload.removeAll(payload);
            }
        }
    }

    public boolean containsImprint(int imprintID){
        return storedImprints.containsKey(imprintID);
    }
    public long getSchemaToImprintLinks() {
        return schemaElementToImprint.values().stream().mapToLong(E -> E.size()).sum();
    }

    public int getSchemaLinks() {
        return schemaElementToImprint.size();
    }

    public int getImprintLinks() {
        return storedImprints.size();
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
    public Result<Boolean> removeSummarizedInstance(int schemaElementID, int imprintID, boolean lightDelete) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                Result<Boolean> result = new Result<>(trackExecutionTimes, trackAllChanges || trackMandatory);
                Set<Integer> imprintIDs = schemaElementToImprint.get(schemaElementID);
                if (imprintIDs != null) {
                    if (imprintIDs.size() > 1) {
                        //still some instances are referenced
                        if (trackAllChanges) {
                            Result<Set<String>> payloadOldResult = getPayload(schemaElementID);
                            Set<String> payloadOld = payloadOldResult._result;
                            result._changeTracker.merge(payloadOldResult._changeTracker);
                            imprintIDs.remove(imprintID);
                            result._changeTracker.incRemovedInstanceToSchemaLinks();
                            schemaElementToImprint.put(schemaElementID, imprintIDs);
                            Result<Set<String>> payloadNewResult = getPayload(schemaElementID);
                            Set<String> payloadNew = payloadNewResult._result;
                            result._changeTracker.merge(payloadNewResult._changeTracker);

                            if (payloadOld.hashCode() != payloadNew.hashCode()) {
                                result._changeTracker.incPayloadElementsChanged();
                                result._changeTracker.incPayloadEntriesRemoved(payloadOld.size() - payloadNew.size());
                            }

                            if (trackExecutionTimes)
                                result._timeSpentDeletingSecondaryIndex += (System.currentTimeMillis() - start);
                            result._result = false;
                        } else {
                            imprintIDs.remove(imprintID);
                            schemaElementToImprint.put(schemaElementID, imprintIDs);
                            result._result = false;
                        }
                    } else {
                        if (trackAllChanges) {
                            //schema element is removed, so is it payload
                            if (!lightDelete) {
                                result._changeTracker.incPayloadElementsChanged();
                                result._changeTracker.incPayloadEntriesRemoved(getPayload(schemaElementID)._result.size());
                            }

                        }
                        Set<Integer> removedImprints = schemaElementToImprint.remove(schemaElementID);
                        if (trackAllChanges && removedImprints != null && !lightDelete) {
                            result._changeTracker.incRemovedInstanceToSchemaLinks(removedImprints.size());
                            result._changeTracker.incRemovedInstanceToSchemaLinks(removedImprints.size());
                        }
                        //NEW, save before delete
                        schemaElementsToBeRemoved.add(schemaElementID);
                        if (trackExecutionTimes)
                            result._timeSpentDeletingSecondaryIndex += (System.currentTimeMillis() - start);

                        result._result = true;
                    }
                } else {
                    if (trackExecutionTimes)
                        result._timeSpentDeletingSecondaryIndex += (System.currentTimeMillis() - start);

                    result._result = true;
                }
                return result;
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
    public Result<Set<Integer>> removeImprints(Set<Imprint> imprints) {
        Result<Set<Integer>> result = new Result<>(trackExecutionTimes, trackAllChanges);
        result._result = new HashSet<>();

        imprints.forEach(I -> {
            Result<Boolean> deleteResult = removeSummarizedInstance(I._schemaElementID, I._id, false);
            if (deleteResult._result)
                result._result.add(I._schemaElementID);

            if (trackAllChanges)
                result.mergeAll(deleteResult);
        });

        long start = System.currentTimeMillis();
        synchronized (readSyncImprint) {
            synchronized (writeSyncImprint) {
                for (Imprint imprint : imprints) {
                    storedImprints.remove(imprint._id);
                    if (trackAllChanges)
                        result._changeTracker.incInstancesDeleted();
                }
                if (trackExecutionTimes)
                    result._timeSpentDeletingSecondaryIndex += (System.currentTimeMillis() - start);
            }
        }
        return result;
    }

    /**
     * This method is called when a new schema structure is freshly build.
     * Only updates relationships between imprints of instances and schema elements
     *
     * @param schemaElementID
     * @param imprintIDs
     */
    public Result<Boolean> putSummarizedInstances(int schemaElementID, Set<Integer> imprintIDs) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                Result<Boolean> result = new Result<>(trackExecutionTimes, trackAllChanges || trackMandatory);
                schemaElementToImprint.put(schemaElementID, imprintIDs);
                result._result = true;
                if (trackAllChanges)
                    result._changeTracker.incAddedInstanceToSchemaLinks(imprintIDs.size());
                if (trackExecutionTimes)
                    result._timeSpentWritingSecondaryIndex += (System.currentTimeMillis() - start);

                return result;
            }
        }
    }

    /**
     * Similar to put, but merges with existing content.
     *
     * @param schemaElementID
     * @param imprintIDs
     */
    public Result<Boolean> addSummarizedInstances(int schemaElementID, Set<Integer> imprintIDs) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                Result<Boolean> result = new Result<>(trackExecutionTimes, trackAllChanges || trackMandatory);
                if (trackAllChanges) {
                    Set<Integer> prev = schemaElementToImprint.get(schemaElementID);
                    if (prev == null)
                        prev = new HashSet<>();
                    int prevSize = prev.size();

                    prev.addAll(imprintIDs);
                    result._changeTracker.incAddedInstanceToSchemaLinks(prev.size() - prevSize);
                    schemaElementToImprint.put(schemaElementID, prev);
                    if (trackExecutionTimes)
                        result._timeSpentWritingSecondaryIndex += (System.currentTimeMillis() - start);
                } else {
                    schemaElementToImprint.merge(schemaElementID, imprintIDs, (O, N) -> {
                        O.addAll(N);
                        return O;
                    });
                }
                result._result = true;
                return result;
            }
        }
    }

    public Result<Boolean> addNodesToSchemaElement(Map<Integer, Set<String>> nodes, Integer schemaHash) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            synchronized (writeSyncSchemaLinks) {
                synchronized (readSyncImprint) {
                    synchronized (writeSyncImprint) {
                        Result<Boolean> result = new Result<>(trackExecutionTimes, trackAllChanges || trackMandatory);
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
                                    Result<Set<String>> tmpResult = updatePayload_local(imprint._payload, node.getValue(), false, false);
                                    imprint._payload = tmpResult._result;
                                    if (trackAllChanges)
                                        result._changeTracker.merge(tmpResult._changeTracker);

                                }
                                //set current time so avoid deletion after completion
                                imprint._timestamp = System.currentTimeMillis();
                                imprint._schemaElementID = schemaHash;
                            } else {
                                //create new one
                                Result<Set<String>> tmpResult = updatePayload_local(new HashSet<>(), node.getValue(), true, false);
                                imprint = new Imprint(node.getKey(), System.currentTimeMillis(), tmpResult._result, schemaHash);
                                storedImprints.put(imprint._id, imprint);
                                if (trackAllChanges) {
                                    result._changeTracker.incInstancesNew();
                                    result._changeTracker.merge(tmpResult._changeTracker);
                                }
                            }

                            //add also a link from schema element to (newly) summarized instance
                            imprintIDs.add(node.getKey());
                            schemaElementToImprint.put(schemaHash, imprintIDs);

                            if (schemaElementsToBeRemoved.contains(schemaHash))
                                schemaElementsToBeRemoved.remove(schemaHash);
                        }
                        if (trackExecutionTimes)
                            result._timeSpentWritingSecondaryIndex += (System.currentTimeMillis() - start);

                        result._result = true;
                        return result;
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
    public Result<Boolean> touchMultiple(Map<Integer, Set<String>> nodes) {
        long start = System.currentTimeMillis();
        synchronized (readSyncImprint) {
            synchronized (writeSyncImprint) {
                Result<Boolean> result = new Result<>(trackExecutionTimes, trackAllChanges || trackMandatory);
                result._result = true;
                for (Map.Entry<Integer, Set<String>> node : nodes.entrySet()) {
                    //update imprint if necessary
                    Imprint imprint = storedImprints.get(node.getKey());
                    if (imprint != null) {
                        //handle payload updates here
                        if (imprint._payload.hashCode() != node.getValue().hashCode()) {
                            //the payload extracted from that instance has changed
                            //set the payload exactly to the new one
                            Result<Set<String>> tmpResult = updatePayload_local(imprint._payload, node.getValue(), false, false);
                            imprint._payload = tmpResult._result;
                            if (trackAllChanges)
                                result._changeTracker.merge(tmpResult._changeTracker);
                        }
                        //set current time so avoid deletion after completion
                        imprint._timestamp = System.currentTimeMillis();
                    } else {
                        logger.error("This should not happen!");
                    }
                }
                if (trackAllChanges || trackMandatory)
                    result._changeTracker.incInstancesNotChanged(nodes.size());
                if (trackExecutionTimes)
                    result._timeSpentWritingSecondaryIndex += (System.currentTimeMillis() - start);

                return result;
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
    public Result<Set<Integer>> removeOldImprints(long timestamp) {
        long start = System.currentTimeMillis();
        Result<Set<Integer>> collectResult = new Result<>(trackExecutionTimes, trackAllChanges);
        Set<Imprint> imprintSet = storedImprints.values().parallelStream().filter(I -> I._timestamp < timestamp).collect(Collectors.toSet());
        if (trackExecutionTimes)
            collectResult._timeSpentReadingSecondaryIndex += (System.currentTimeMillis() - start);

        Result<Set<Integer>> deleteResult = removeImprints(imprintSet);
        if (trackExecutionTimes)
            deleteResult.mergeTimes(collectResult);

        return deleteResult;
    }


    //GETTER:

    public Result<Integer> getSchemaElementFromImprintID(int imprintID) {
        long start = System.currentTimeMillis();
        synchronized (readSyncImprint) {
            Result<Integer> result = new Result<>(trackExecutionTimes, trackAllChanges);
            Imprint imprint = storedImprints.get(imprintID);
            if (trackExecutionTimes)
                result._timeSpentReadingSecondaryIndex += (System.currentTimeMillis() - start);

            if (imprint != null)
                result._result = imprint._schemaElementID;

            return result;
        }
    }


    /**
     * Read only! Returns the summarized instances from the schema element.
     *
     * @param schemaElementID
     * @return
     */
    public Result<Set<Imprint>> getSummarizedInstances(int schemaElementID) {
        long start = System.currentTimeMillis();
        synchronized (readSyncSchemaLinks) {
            Result<Set<Imprint>> result = new Result<>(trackExecutionTimes, trackAllChanges);
            Set<Integer> imprintIDs = schemaElementToImprint.get(schemaElementID);
            if (trackExecutionTimes)
                result._timeSpentReadingSecondaryIndex += (System.currentTimeMillis() - start);
            if (imprintIDs != null) {
                start = System.currentTimeMillis();
                synchronized (readSyncImprint) {
                    Set<Imprint> resultImprints = new HashSet<>();
                    for (Integer imprintID : imprintIDs) {
                        Imprint imp = storedImprints.get(imprintID);
                        if (imp != null)
                            resultImprints.add(imp);
                    }
                    if (trackExecutionTimes)
                        result._timeSpentReadingSecondaryIndex += (System.currentTimeMillis() - start);
                    result._result = resultImprints;
                    return result;
                }
            } else
                return result;
        }
    }

    public boolean checkSchemaElement(int schemaHash) {
        return schemaElementToImprint.containsKey(schemaHash);
    }

    /**
     * @param schemaHash
     * @return
     */
    public Result<Set<String>> getPayload(int schemaHash) {
        Result<Set<String>> result = new Result<>(trackExecutionTimes, trackAllChanges);
        Result<Set<Imprint>> imprints = getSummarizedInstances(schemaHash);
        if (trackExecutionTimes)
            result.mergeTimes(imprints);

        long start = System.currentTimeMillis();
        Set<String> payload = new HashSet<>();
        if (imprints != null && imprints._result != null) {
            for (Imprint imprint : imprints._result)
                if (imprint._payload != null)
                    payload.addAll(imprint._payload);
        }
        if (trackExecutionTimes)
            result._timeSpentReadingSecondaryIndex += (System.currentTimeMillis() - start);
        result._result = payload;
        return result;
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private Result<Set<String>> _updatePayMinimalTrack(Set<String> oldPayload, Set<String> newPayload, boolean addOnly, boolean removeOnly) {
        Result<Set<String>> result = new Result<>(trackExecutionTimes, trackAllChanges);
        if (removeOnly) {
            if(oldPayload.removeAll(newPayload))
                result._changeTracker.incPayloadElementsChanged();

            result._result = oldPayload;
            return result;
        } else {
            if (addOnly) {
                int h1 = oldPayload.hashCode();
                oldPayload.addAll(newPayload);
                if (h1 != oldPayload.hashCode())
                    result._changeTracker.incPayloadElementsChanged();

                result._result = oldPayload;
                return result;
            } else {
                if (oldPayload.hashCode() != newPayload.hashCode())
                    result._changeTracker.incPayloadElementsChanged();

                result._result = newPayload;
                return result;
            }
        }

    }


    private Result<Set<String>> _updatePayFullTrack(Set<String> oldPayload, Set<String> newPayload, boolean addOnly, boolean removeOnly){
        Result<Set<String>> result = new Result<>(trackExecutionTimes, trackAllChanges);
        if (removeOnly) {
            int before = oldPayload.size();
            oldPayload.removeAll(newPayload);
            int deletions = before - oldPayload.size();
            if (deletions > 0) {
                result._changeTracker.incPayloadElementsChanged();
                result._changeTracker.incPayloadEntriesRemoved(deletions);
            }
            result._result = oldPayload;
            return result;
        } else {
            if (addOnly) {
                int before = oldPayload.size();
                oldPayload.addAll(newPayload);
                int additions = oldPayload.size() - before;
                if (additions > 0) {
                    result._changeTracker.incPayloadElementsChanged();
                    result._changeTracker.incPayloadEntriesAdded(additions);
                }
                result._result = oldPayload;
                return result;
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
                    result._changeTracker.incPayloadElementsChanged();
                    result._changeTracker.incPayloadEntriesAdded(additions);
                    result._changeTracker.incPayloadEntriesRemoved(deletions);
                }
                result._result = newPayload;
                return result;
            }
        }
    }

    private Result<Set<String>> updatePayload_local(Set<String> oldPayload, Set<String> newPayload, boolean addOnly, boolean removeOnly) {
        if (!trackAllChanges) {
            Result<Set<String>> result = new Result<>(trackExecutionTimes, trackAllChanges);
            if (addOnly) {
                oldPayload.addAll(newPayload);
                result._result = oldPayload;
                return result;
            } else if (removeOnly) {
                oldPayload.removeAll(newPayload);
                result._result = oldPayload;
                return result;
            } else {
                result._result = newPayload;
                return result;
            }
        } else {
            if(TRACK_PAYLOAD_DETAILS)
                return _updatePayFullTrack(oldPayload, newPayload, addOnly, removeOnly);
            else
            return _updatePayMinimalTrack(oldPayload, newPayload, addOnly, removeOnly);
        }
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        storedImprints.forEach((k, v) -> sb.append(v.toString() + "\n"));
        schemaElementToImprint.forEach((k, v) -> sb.append("{" + k + "->" + v.toString() + "}"));
        return sb.toString();
    }

}
