package database;

import java.util.HashMap;
import java.util.HashSet;

public class ChangeTracker {

    //the number of instances with the same schema element this iteration
    private HashMap<Integer,Integer> schemaElementsThisIteration = new HashMap();

    //all schema hashes that where written to db this iteration
    private HashSet<Integer> schemaElementsAddedThisIteration = new HashSet<>();
    //all schema hashes that where deleted from db this iteration
    private HashSet<Integer> schemaElementsDeletedThisIteration = new HashSet<>();

    //all instance hashes that had a change this iteration
    private HashSet<Integer> instancesChangedThisIteration = new HashSet<>();
    //all instance hashes that were affected by a change of a neighbor
    private HashSet<Integer> instancesChangedBecauseOfNeighbors = new HashSet<>();
    //all instance hashes that had a change this iteration
    private HashSet<Integer> instancesNewThisIteration = new HashSet<>();
    //all instance hashes that were deleted this iteration
    private HashSet<Integer> instancesDeletedThisIteration = new HashSet<>();
    //the number of instances that did not change at all
    private Integer instancesNotChangedThisIteration  = 0;

    //updates on the coordinator
    private Integer addedInstancesSchemaLinks = 0;
    private Integer removedInstancesSchemaLinks = 0;

    //total number of schema elements that had a payload change
    private Integer payloadElementsChangedThisIteration = 0;
    //in detail: was something added or removed
    private Integer payloadEntriesAdded = 0;
    private Integer payloadEntriesRemoved = 0;


    public HashMap<Integer, Integer> getSchemaElementsThisIteration() {
        return schemaElementsThisIteration;
    }

    public void setSchemaElementsThisIteration(HashMap<Integer, Integer> schemaElementsThisIteration) {
       schemaElementsThisIteration = schemaElementsThisIteration;
    }

    public HashSet<Integer> getSchemaElementsAddedThisIteration() {
        return schemaElementsAddedThisIteration;
    }

    public void setSchemaElementsAddedThisIteration(HashSet<Integer> schemaElementsAddedThisIteration) {
        schemaElementsAddedThisIteration = schemaElementsAddedThisIteration;
    }

    public HashSet<Integer> getInstancesChangedThisIteration() {
        return instancesChangedThisIteration;
    }

    public void setInstancesChangedThisIteration(HashSet<Integer> instancesChangedThisIteration) {
        instancesChangedThisIteration = instancesChangedThisIteration;
    }

    public int getInstancesNotChangedThisIteration() {
        return instancesNotChangedThisIteration;
    }

    public void setInstancesNotChangedThisIteration(int instancesNotChangedThisIteration) {
        instancesNotChangedThisIteration = instancesNotChangedThisIteration;
    }

    public void incInstancesNotChangedThisIteration() {
        instancesNotChangedThisIteration++;
    }

    public HashSet<Integer> getInstancesNewThisIteration() {
        return instancesNewThisIteration;
    }

    public void setInstancesNewThisIteration(HashSet<Integer> instancesNewThisIteration) {
        instancesNewThisIteration = instancesNewThisIteration;
    }

    public HashSet<Integer> getSchemaElementsDeletedThisIteration() {
        return schemaElementsDeletedThisIteration;
    }

    public void setSchemaElementsDeletedThisIteration(HashSet<Integer> schemaElementsDeletedThisIteration) {
        schemaElementsDeletedThisIteration = schemaElementsDeletedThisIteration;
    }

    public Integer getAddedInstancesSchemaLinks() {
        return addedInstancesSchemaLinks;
    }

    public void setAddedInstancesSchemaLinks(Integer addedInstancesSchemaLinks) {
        addedInstancesSchemaLinks = addedInstancesSchemaLinks;
    }
    public void incAddedInstancesSchemaLinks() {
        addedInstancesSchemaLinks++;
    }

    public Integer getRemovedInstancesSchemaLinks() {
        return removedInstancesSchemaLinks;
    }

    public void setRemovedInstancesSchemaLinks(Integer removedInstancesSchemaLinks) {
        removedInstancesSchemaLinks = removedInstancesSchemaLinks;
    }
    public void incRemovedInstancesSchemaLinks() {
        removedInstancesSchemaLinks++;
    }



    public String pprintSimple() {
        String string = "";
        string += "schemaElementsAddedThisIteration: " + schemaElementsAddedThisIteration.size() + "\n";
        string += "schemaElementsDeletedThisIteration: " + schemaElementsDeletedThisIteration.size()+ "\n";
        string += "-------------------------------------+ \n";
        string += "payloadElementsChangedThisIteration: " + payloadElementsChangedThisIteration + "\n";
        string += "payloadEntriesAddedThisIteration: " + payloadEntriesAdded + "\n";
        string += "payloadEntriesRemovedThisIteration: " + payloadEntriesRemoved + "\n";
        string += "-------------------------------------+ \n";
        string += "instancesChangedThisIteration: " + instancesChangedThisIteration.size()+ "\n";
        string += "instancesChangedBecauseOfNeighbors: " + instancesChangedBecauseOfNeighbors.size()+ "\n";
        string += "instancesNotChangedThisIteration: " + instancesNotChangedThisIteration+ "\n";
        string += "instancesNewThisIteration: " + instancesNewThisIteration.size()+ "\n";
        string += "instancesDeletedThisIteration: " + instancesDeletedThisIteration.size()+ "\n";
        string += "-------------------------------------+ \n";
        string += "addedInstancesSchemaLinks: " + addedInstancesSchemaLinks+ "\n";
        string += "removedInstancesSchemaLinks: " + removedInstancesSchemaLinks+ "\n";
        return string;
    }


    public Integer getPayloadElementsChangedThisIteration() {
        return payloadElementsChangedThisIteration;
    }

    public void incPayloadElementsChangedThisIteration() {
        payloadElementsChangedThisIteration++;
    }

    public Integer getPayloadEntriesAdded() {
        return payloadEntriesAdded;
    }

    public void incPayloadEntriesAdded(Integer payloadEntriesAdded) {
        payloadEntriesAdded += payloadEntriesAdded;
    }

    public Integer getPayloadEntriesRemoved() {
        return payloadEntriesRemoved;
    }

    public void incPayloadEntriesRemoved(Integer payloadEntriesRemoved) {
        payloadEntriesRemoved += payloadEntriesRemoved;
    }

    public HashSet<Integer> getInstancesDeletedThisIteration() {
        return instancesDeletedThisIteration;
    }

    public HashSet<Integer> getInstancesChangedBecauseOfNeighbors() {
        return instancesChangedBecauseOfNeighbors;
    }

    public void setInstancesChangedBecauseOfNeighbors(HashSet<Integer> instancesChangedBecauseOfNeighbors) {
        instancesChangedBecauseOfNeighbors = instancesChangedBecauseOfNeighbors;
    }
}
