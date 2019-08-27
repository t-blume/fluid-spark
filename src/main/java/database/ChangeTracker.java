package database;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class ChangeTracker {

    private static ChangeTracker singletonInstance = null;

    public static ChangeTracker getInstance(){
        if(singletonInstance == null)
            singletonInstance = new ChangeTracker();
        return singletonInstance;
    }


    //a instance is observed with a new schema (SE_new)
    public Integer _newSchemaStructureObserved = 0;

    //no more instance with a specific schema exists in the data graph (SE_del)
    public Integer _schemaStructureDeleted = 0;


    //number of instance with a changed schema
    //a known instance is observed with a changed schema (SE_mod)
    public Integer _instancesWithChangedSchema = 0;

    //a new instance is observed with a known schema (PE_add)
    public Integer _instancesNewWithKnownSchema = 0;


    //a instance with its schema and payload information no longer exists (PE_del)
    public Integer _instancesDeleted = 0;

    //a known instance is observed with (at most) only changed instance information (PE_mod)
    public Integer _instancesNotChanged = 0;


    /***************************
     * More fine grained stats *
     ***************************/
    //number of schema elements written to db
    public Integer _schemaElementsAdded = 0;

    //number of schema elements deleted from db
    public Integer _schemaElementsDeleted = 0;

    //number of instances newly added
    public Integer _instancesNew = 0;

    //number of instances affected by a change of a neighbor
    public Integer _instancesChangedBecauseOfNeighbors = 0;

    //updates on the coordinator
    public Integer _addedInstanceToSchemaLinks = 0;
    public Integer _removedInstanceToSchemaLinks = 0;

    //total number of payload elements that had a change
    public Integer _payloadElementsChanged = 0;

    //in detail: was something added or removed
    public Integer _payloadEntriesAdded = 0;
    public Integer _payloadEntriesRemoved = 0;


    public void resetScores() {
        _newSchemaStructureObserved = 0;
        _schemaStructureDeleted = 0;
        _schemaElementsAdded = 0;
        _schemaElementsDeleted = 0;
        _instancesWithChangedSchema = 0;
        _instancesChangedBecauseOfNeighbors = 0;
        _instancesNew = 0;
        _instancesDeleted = 0;
        _instancesNotChanged = 0;
        _addedInstanceToSchemaLinks = 0;
        _removedInstanceToSchemaLinks = 0;
        _payloadElementsChanged = 0;
        _payloadEntriesAdded = 0;
        _payloadEntriesRemoved = 0;
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
            for(int i=0; i < header.length -1; i++){
                writer.write(header[i] + delimiter);
            }
            writer.write(header[header.length-1]);
            writer.newLine();
        }
        String contentLine = "";

        contentLine += addContent(iteration, delimiter);
        /*****************************
         * primary reporting numbers *
         *****************************/
        contentLine += addContent(_newSchemaStructureObserved, delimiter);
        contentLine += addContent(_schemaElementsDeleted, delimiter);
        contentLine += addContent(_instancesWithChangedSchema, delimiter);
        contentLine += addContent(_instancesNewWithKnownSchema, delimiter);
        contentLine += addContent(_instancesDeleted, delimiter);
        contentLine += addContent(_instancesNotChanged, delimiter);
        /***************************
         * More fine grained stats *
         ***************************/
        contentLine += addContent(_schemaElementsAdded, delimiter);
        contentLine += addContent(_schemaElementsDeleted, delimiter);
        contentLine += addContent(_instancesNew, delimiter);
        contentLine += addContent(_instancesChangedBecauseOfNeighbors, delimiter);
        contentLine += addContent(_payloadElementsChanged, delimiter);
        contentLine += addContent(_payloadEntriesAdded, delimiter);
        contentLine += addContent(_payloadEntriesRemoved, delimiter);
        /*********************
         * UpdateCoordinator *
         *********************/
        contentLine += addContent(_addedInstanceToSchemaLinks, delimiter);
        contentLine += addContent(_removedInstanceToSchemaLinks, ' ');


        writer.write(contentLine);
        writer.newLine();
        writer.close();

    }

    private static String addContent(int number, char delimiter){
        return String.valueOf(number) + delimiter;
    }

    public String pprintSimple() {
        String string = "";
        string += "SchemaElementsAdded (SE_new): " + _schemaElementsAdded + "\n";
        string += "SchemaElementsDeleted (SE_del): " + _schemaElementsDeleted + "\n";
        string += "-------------------------------------+ \n";
        string += "PayloadElementsChanged: " + _payloadElementsChanged + "\n";
        string += "PayloadEntriesAddedThisIteration: " + _payloadEntriesAdded + "\n";
        string += "PayloadEntriesRemovedThisIteration: " + _payloadEntriesRemoved + "\n";
        string += "-------------------------------------+ \n";
        string += "InstancesWithChangedSchema (SE_mod): " + _instancesWithChangedSchema + "\n";
        string += "InstancesChangedBecauseOfNeighbors: " + _instancesChangedBecauseOfNeighbors + "\n";
        string += "InstancesNewWithKnownSchema (PE_add): " + _instancesNewWithKnownSchema + "\n";
        string += "InstancesKnownWithKnownSchema (PE_mod): " + _instancesNotChanged + "\n";
        string += "InstancesNew: " + _instancesNew + "\n";
        string += "InstancesDeleted (PE_del): " + _instancesDeleted + "\n";
        string += "-------------------------------------+ \n";
        string += "AddedInstanceToSchemaLinks: " + _addedInstanceToSchemaLinks + "\n";
        string += "RemovedInstanceToSchemaLinks: " + _removedInstanceToSchemaLinks + "\n";
        return string;
    }
}
