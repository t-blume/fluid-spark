package schema;

import java.util.HashMap;
import java.util.HashSet;

public class ChangeTracker {

    /*
    TODO: what about deletion?
     */
    //the number of instances with the same schema element this iteration
    private static HashMap<Integer,Integer> schemaElementsThisIteration = new HashMap();
    //all schema hashes that where written to db this iteration
    private static HashSet<Integer> schemaElementsAddedThisIteration = new HashSet<>();
    //all schema hashes that where deleted from db this iteration
    private static HashSet<Integer> schemaElementsDeletedThisIteration = new HashSet<>();

    //all instance hashes that had a change this iteration
    private static HashSet<Integer> instancesChangedThisIteration = new HashSet<>();
    //all instance hashes that had a change this iteration
    private static HashSet<Integer> instancesNewThisIteration = new HashSet<>();
    //the number of instances that did not change at all
    private static Integer instancesNotChangedThisIteration  = 0;


    private static Integer addedInstancesSchemaLinks = 0;
    private static Integer removedInstancesSchemaLinks = 0;


    private static Integer payloadElementsChangedThisIteration = 0;
    private static Integer payloadEntriesAdded = 0;
    private static Integer payloadEntriesRemoved = 0;


    public static HashMap<Integer, Integer> getSchemaElementsThisIteration() {
        return schemaElementsThisIteration;
    }

    public static void setSchemaElementsThisIteration(HashMap<Integer, Integer> schemaElementsThisIteration) {
        ChangeTracker.schemaElementsThisIteration = schemaElementsThisIteration;
    }

    public static HashSet<Integer> getSchemaElementsAddedThisIteration() {
        return schemaElementsAddedThisIteration;
    }

    public static void setSchemaElementsAddedThisIteration(HashSet<Integer> schemaElementsAddedThisIteration) {
        ChangeTracker.schemaElementsAddedThisIteration = schemaElementsAddedThisIteration;
    }

    public static HashSet<Integer> getInstancesChangedThisIteration() {
        return instancesChangedThisIteration;
    }

    public static void setInstancesChangedThisIteration(HashSet<Integer> instancesChangedThisIteration) {
        ChangeTracker.instancesChangedThisIteration = instancesChangedThisIteration;
    }

    public static int getInstancesNotChangedThisIteration() {
        return instancesNotChangedThisIteration;
    }

    public static void setInstancesNotChangedThisIteration(int instancesNotChangedThisIteration) {
        ChangeTracker.instancesNotChangedThisIteration = instancesNotChangedThisIteration;
    }

    public static void incInstancesNotChangedThisIteration() {
        ChangeTracker.instancesNotChangedThisIteration++;
    }

    public static HashSet<Integer> getInstancesNewThisIteration() {
        return instancesNewThisIteration;
    }

    public static void setInstancesNewThisIteration(HashSet<Integer> instancesNewThisIteration) {
        ChangeTracker.instancesNewThisIteration = instancesNewThisIteration;
    }

    public static HashSet<Integer> getSchemaElementsDeletedThisIteration() {
        return schemaElementsDeletedThisIteration;
    }

    public static void setSchemaElementsDeletedThisIteration(HashSet<Integer> schemaElementsDeletedThisIteration) {
        ChangeTracker.schemaElementsDeletedThisIteration = schemaElementsDeletedThisIteration;
    }

    public static Integer getAddedInstancesSchemaLinks() {
        return addedInstancesSchemaLinks;
    }

    public static void setAddedInstancesSchemaLinks(Integer addedInstancesSchemaLinks) {
        ChangeTracker.addedInstancesSchemaLinks = addedInstancesSchemaLinks;
    }
    public static void incAddedInstancesSchemaLinks() {
        ChangeTracker.addedInstancesSchemaLinks++;
    }

    public static Integer getRemovedInstancesSchemaLinks() {
        return removedInstancesSchemaLinks;
    }

    public static void setRemovedInstancesSchemaLinks(Integer removedInstancesSchemaLinks) {
        ChangeTracker.removedInstancesSchemaLinks = removedInstancesSchemaLinks;
    }
    public static void incRemovedInstancesSchemaLinks() {
        ChangeTracker.removedInstancesSchemaLinks++;
    }



    public static String pprintSimple() {
        String string = "";
        string += "schemaElementsAddedThisIteration: " + schemaElementsAddedThisIteration.size() + "\n";
        string += "schemaElementsDeletedThisIteration: " + schemaElementsDeletedThisIteration.size()+ "\n";
        string += "-------------------------------------+ \n";
        string += "payloadElementsChangedThisIteration: " + payloadElementsChangedThisIteration + "\n";
        string += "payloadEntriesAddedThisIteration: " + payloadEntriesAdded + "\n";
        string += "payloadEntriesRemovedThisIteration: " + payloadEntriesRemoved + "\n";
        string += "-------------------------------------+ \n";
        string += "instancesChangedThisIteration: " + instancesChangedThisIteration.size()+ "\n";
        string += "instancesNotChangedThisIteration: " + instancesNotChangedThisIteration+ "\n";
        string += "instancesNewThisIteration: " + instancesNewThisIteration.size()+ "\n";
        string += "-------------------------------------+ \n";
        string += "addedInstancesSchemaLinks: " + addedInstancesSchemaLinks+ "\n";
        string += "removedInstancesSchemaLinks: " + removedInstancesSchemaLinks+ "\n";
        return string;
    }


    public static Integer getPayloadElementsChangedThisIteration() {
        return payloadElementsChangedThisIteration;
    }

    public static void incPayloadElementsChangedThisIteration() {
        ChangeTracker.payloadElementsChangedThisIteration++;
    }

    public static Integer getPayloadEntriesAdded() {
        return payloadEntriesAdded;
    }

    public static void incPayloadEntriesAdded(Integer payloadEntriesAdded) {
        ChangeTracker.payloadEntriesAdded += payloadEntriesAdded;
    }

    public static Integer getPayloadEntriesRemoved() {
        return payloadEntriesRemoved;
    }

    public static void incPayloadEntriesRemoved(Integer payloadEntriesRemoved) {
        ChangeTracker.payloadEntriesRemoved += payloadEntriesRemoved;
    }
}
