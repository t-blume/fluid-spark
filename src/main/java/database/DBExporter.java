package database;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class DBExporter {
    public static void main(String[] args) throws FileNotFoundException {
        //TODO as command line args
        String database = "till-test";

        String outputDir = "export";
        new File(outputDir).mkdirs();

        String output = outputDir + "/" + database + ".nt";
        OrientConnector.create(database, false);
        OrientConnector connector = OrientConnector.getInstance(database, false, false);
        connector.open();
        // test stuff
//        long[] counts = connector.countSchemaElementsAndLinks();
//        System.out.println(counts[0]);
//        System.out.println(counts[1]);

        // Creates a FileOutputStream
        FileOutputStream fs = new FileOutputStream(output);

        // Creates a PrintStream
        long exportedTriples = connector.exportGraphAsNTriples(null, "type", new PrintStream(fs, true)); //System.out
        System.out.println("Exported " + exportedTriples + " triples to " + output);
    }
}
