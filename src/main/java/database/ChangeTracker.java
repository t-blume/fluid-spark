package database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ChangeTracker {

    private static ChangeTracker singletonInstance = null;

    public static ChangeTracker getInstance() {
        if (singletonInstance == null)
            singletonInstance = new ChangeTracker();
        return singletonInstance;
    }


    //a instance is observed with a new schema (SE_new)
    private Integer newSchemaStructureObserved = 0;

    //no more instance with a specific schema exists in the data graph (SE_del)
    private Integer schemaStructureDeleted = 0;


    //number of instance with a changed schema
    //a known instance is observed with a changed schema (SE_mod)
    private Integer instancesWithChangedSchema = 0;

    //a new instance is observed with a known schema (PE_add)
    private Integer instancesNewWithKnownSchema = 0;


    //a instance with its schema and payload information no longer exists (PE_del)
    private Integer instancesDeleted = 0;

    //a known instance is observed with (at most) only changed instance information (PE_mod)
    private Integer instancesNotChanged = 0;


    /***************************
     * More fine grained stats *
     ***************************/
    //number of schema elements written to db
    private Integer schemaElementsAdded = 0;

    //number of schema elements deleted from db
    private Integer schemaElementsDeleted = 0;

    //number of instances newly added
    private Integer instancesNew = 0;

    //number of instances affected by a change of a neighbor
    private Integer instancesChangedBecauseOfNeighbors = 0;

    //updates on the coordinator
    private Integer addedInstanceToSchemaLinks = 0;
    private Integer removedInstanceToSchemaLinks = 0;

    //total number of payload elements that had a change
    private Integer payloadElementsChanged = 0;

    //in detail: was something added or removed
    private Integer payloadEntriesAdded = 0;
    private Integer payloadEntriesRemoved = 0;


    public Integer getNewSchemaStructureObserved() {
        return newSchemaStructureObserved;
    }

    public Integer getSchemaStructureDeleted() {
        return schemaStructureDeleted;
    }

    public Integer getInstancesWithChangedSchema() {
        return instancesWithChangedSchema;
    }

    public Integer getInstancesNewWithKnownSchema() {
        return instancesNewWithKnownSchema;
    }

    public Integer getInstancesDeleted() {
        return instancesDeleted;
    }

    public Integer getInstancesNotChanged() {
        return instancesNotChanged;
    }

    public Integer getSchemaElementsAdded() {
        return schemaElementsAdded;
    }

    public Integer getSchemaElementsDeleted() {
        return schemaElementsDeleted;
    }

    public Integer getInstancesNew() {
        return instancesNew;
    }

    public Integer getInstancesChangedBecauseOfNeighbors() {
        return instancesChangedBecauseOfNeighbors;
    }

    public Integer getAddedInstanceToSchemaLinks() {
        return addedInstanceToSchemaLinks;
    }

    public Integer getRemovedInstanceToSchemaLinks() {
        return removedInstanceToSchemaLinks;
    }

    public Integer getPayloadElementsChanged() {
        return payloadElementsChanged;
    }

    public Integer getPayloadEntriesAdded() {
        return payloadEntriesAdded;
    }

    public Integer getPayloadEntriesRemoved() {
        return payloadEntriesRemoved;
    }

    public void incInstancesNew() {
        synchronized (instancesNew) {
            instancesNew++;
        }
    }

    public void incInstancesNotChanged() {
        incInstancesNotChanged(1);
    }

    public void incInstancesNotChanged(int increment) {
        synchronized (instancesNotChanged) {
            instancesNotChanged += increment;
        }
    }

    public void incInstancesWithChangedSchema() {
        synchronized (instancesWithChangedSchema) {
            instancesWithChangedSchema++;
        }
    }

    public void incInstancesChangedBecauseOfNeighbors() {
        synchronized (instancesChangedBecauseOfNeighbors) {
            instancesChangedBecauseOfNeighbors++;
        }
    }

    public void incSchemaElementsDeleted() {
        synchronized (schemaElementsDeleted) {
            schemaElementsDeleted++;
        }
    }

    public void incNewSchemaStructureObserved() {
        synchronized (newSchemaStructureObserved) {
            newSchemaStructureObserved++;
        }
    }

    public void incSchemaStructureDeleted() {
        incSchemaStructureDeleted(1);
    }
    public void incSchemaStructureDeleted(int increment) {
        synchronized (schemaStructureDeleted) {
            schemaStructureDeleted+=increment;
        }
    }


    public void incSchemaElementsAdded() {
        synchronized (schemaElementsAdded) {
            schemaElementsAdded++;
        }
    }

    public void incRemovedInstanceToSchemaLinks() {
        incRemovedInstanceToSchemaLinks(1);
    }

    public void incRemovedInstanceToSchemaLinks(int increment) {
        synchronized (removedInstanceToSchemaLinks) {
            removedInstanceToSchemaLinks += increment;
        }
    }

    public void incAddedInstanceToSchemaLinks() {
        incAddedInstanceToSchemaLinks(1);
    }

    public void incAddedInstanceToSchemaLinks(int increment) {
        synchronized (addedInstanceToSchemaLinks) {
            addedInstanceToSchemaLinks += increment;
        }
    }

    public void incPayloadElementsChanged() {
        synchronized (payloadElementsChanged) {
            payloadElementsChanged++;
        }
    }

    public void incPayloadEntriesRemoved(int increment) {
        synchronized (payloadEntriesRemoved) {
            payloadEntriesRemoved += increment;
        }
    }

    public void incPayloadEntriesAdded(int increment) {
        synchronized (payloadEntriesAdded) {
            payloadEntriesAdded += increment;
        }
    }

    public void resetScores() {
        newSchemaStructureObserved = 0;
        schemaStructureDeleted = 0;
        schemaElementsAdded = 0;
        schemaElementsDeleted = 0;
        instancesWithChangedSchema = 0;
        instancesChangedBecauseOfNeighbors = 0;
        instancesNew = 0;
        instancesDeleted = 0;
        instancesNotChanged = 0;
        addedInstanceToSchemaLinks = 0;
        removedInstanceToSchemaLinks = 0;
        payloadElementsChanged = 0;
        payloadEntriesAdded = 0;
        payloadEntriesRemoved = 0;
    }

    public void exportToCSV(String filepath, int iteration) throws IOException {
        File file = new File(filepath);
        char delimiter = ';';

        String[] header = new String[]{"Iteration", "NewlyObservedSchema (SE_new)",
                "DeletedSchemaStructures (SE_del)", "ChangedSchemaStructures (SE_mod)",
                "InstanceAddedWithKnownSchema (PE_add)", "InstancesDeleted (PE_del)",
                "InstanceNotChanged (PE_mod)",

                "TotalNumberOfSchemaElementsWritten", "TotalNumberOfSchemaElementsDeleted",
                "TotalNumberOfNewInstances", "ChangedSchemaStructuresBecauseOfNeighbor",
                "TotalNumberOfChangedPayloadElements (real PE_mod)", "PayloadEntriesAdded",
                "PayloadEntriesRemoved",

                "InstanceToSchemaLinksAdded", "InstanceToSchemaLinksRemoved"
        };
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, iteration > 0));
        if (iteration <= 0) {
            //write headers
            for (int i = 0; i < header.length - 1; i++) {
                writer.write(header[i] + delimiter);
            }
            writer.write(header[header.length - 1]);
            writer.newLine();
        }
        String contentLine = "";

        contentLine += addContent(iteration, delimiter);
        /*****************************
         * primary reporting numbers *
         *****************************/
        contentLine += addContent(newSchemaStructureObserved, delimiter);
        contentLine += addContent(schemaStructureDeleted, delimiter);
        contentLine += addContent(instancesWithChangedSchema, delimiter);
        contentLine += addContent(instancesNewWithKnownSchema, delimiter);
        contentLine += addContent(instancesDeleted, delimiter);
        contentLine += addContent(instancesNotChanged, delimiter);
        /***************************
         * More fine grained stats *
         ***************************/
        contentLine += addContent(schemaElementsAdded, delimiter);
        contentLine += addContent(schemaElementsDeleted, delimiter);
        contentLine += addContent(instancesNew, delimiter);
        contentLine += addContent(instancesChangedBecauseOfNeighbors, delimiter);
        contentLine += addContent(payloadElementsChanged, delimiter);
        contentLine += addContent(payloadEntriesAdded, delimiter);
        contentLine += addContent(payloadEntriesRemoved, delimiter);
        /*********************
         * UpdateCoordinator *
         *********************/
        contentLine += addContent(addedInstanceToSchemaLinks, delimiter);
        contentLine += addContent(removedInstanceToSchemaLinks, ' ');


        writer.write(contentLine);
        writer.newLine();
        writer.close();

    }

    private static String addContent(int number, char delimiter) {
        return String.valueOf(number) + delimiter;
    }

    public String pprintSimple() {
        String string = "";
        string += "SchemaElementsAdded (SE_new): " + newSchemaStructureObserved + "\n";
        string += "SchemaElementsDeleted (SE_del): " + schemaStructureDeleted + "\n";
        string += "-------------------------------------+ \n";
        string += "PayloadElementsChanged: " + payloadElementsChanged + "\n";
        string += "PayloadEntriesAddedThisIteration: " + payloadEntriesAdded + "\n";
        string += "PayloadEntriesRemovedThisIteration: " + payloadEntriesRemoved + "\n";
        string += "-------------------------------------+ \n";
        string += "InstancesWithChangedSchema (SE_mod): " + instancesWithChangedSchema + "\n";
        string += "InstancesChangedBecauseOfNeighbors: " + instancesChangedBecauseOfNeighbors + "\n";
        string += "InstancesNewWithKnownSchema (PE_add): " + instancesNewWithKnownSchema + "\n";
        string += "instancesNotChanged (PE_mod): " + instancesNotChanged + "\n";
        string += "InstancesNew: " + instancesNew + "\n";
        string += "InstancesDeleted (PE_del): " + instancesDeleted + "\n";
        string += "-------------------------------------+ \n";
        string += "AddedInstanceToSchemaLinks: " + addedInstanceToSchemaLinks + "\n";
        string += "RemovedInstanceToSchemaLinks: " + removedInstanceToSchemaLinks + "\n";
        return string;
    }
}
