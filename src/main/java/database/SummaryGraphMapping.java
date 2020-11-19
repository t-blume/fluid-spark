package database;

import input.NTripleParser;
import org.apache.spark.graphx.Edge;
import scala.Tuple4;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

public class SummaryGraphMapping {
    public static void main(String[] args) throws IOException {
        String graphSummaryFile = "export/till-test.nt";

        SecondaryIndex secondaryIndex = SecondaryIndex.instantiate(false, false,
                false, "secondaryIndex.ser.gz", true);

        mapSummaryGraph(graphSummaryFile, secondaryIndex);
    }


    public static void mapSummaryGraph(String graphSummaryFile, SecondaryIndex secondaryIndex) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(graphSummaryFile));
        String line;
        while ((line = br.readLine()) != null) {
            Edge<Tuple4<String,String,String,String>> edge = NTripleParser.parse(line);

            String subject = edge.attr()._1();
            String predicate = edge.attr._2();
            String object = edge.attr._3();
            int summaryHash = Integer.valueOf(subject);
            if (secondaryIndex.checkSchemaElement(summaryHash)){
                Result<Set<Imprint>> vertices = secondaryIndex.getSummarizedInstances(summaryHash);
                for (Imprint imprint : vertices._result){

                    System.out.println(subject + ',' + imprint._id);
                }
            }
        }
    }
}
