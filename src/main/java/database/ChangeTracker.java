package database;

public class ChangeTracker {


    //number of schema elements written to db
    // TODO: NOT: a new instance is observed with a new schema (SE_new)
    public Integer _schemaElementsAdded = 0;

    //number of schema elements deleted from db
    //TODO: NOT: no more instance with a specific schema exists in the data graph (SE_del)
    public Integer _schemaElementsDeleted = 0;


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
