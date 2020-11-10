package database;


public class DBExporter {
    public static void main(String[] args){
        //TODO as command line args
        String database = "till-test";

        OrientConnector.create(database, false);
        OrientConnector connector = OrientConnector.getInstance(database, false, false);
        connector.open();
        long[] counts = connector.countSchemaElementsAndLinks();
        System.out.println(counts[0]);
        System.out.println(counts[1]);

        connector.exportGraphAsNTriples(null, "type", System.out);
    }
}
