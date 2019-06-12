package schema;

import graph.Edge;
import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TypesAndPropertiesTest extends TestCase {

    private String[] filenames = new String[]{"resources/manual-test-2.nq"};

    private HashMap<String, Set<Edge>> instances = new HashMap<>();

    public void setUp() throws Exception {
        super.setUp();
        for (String filename : filenames) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(filename));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            String line = null;
            while (true) {
                try {
                    if (!((line = reader.readLine()) != null)) break;
                } catch (IOException e) {
                    e.printStackTrace();
                }
                Edge edge = Edge.fromNQuad(line);
                if (edge != null) {
                    Set<Edge> set = new HashSet<>();
                    set.add(edge);
                    instances.merge(edge.start, set, (O, N) -> {
                        O.addAll(N);
                        return O;
                    });
                }
            }
        }
    }

    public void testFromInstance() {
        Map<Integer, Set<ISchemaElement>> schemaElements = new HashMap<>();
        for(Map.Entry<String, Set<Edge>> instance : instances.entrySet()){
            ISchemaElement schemaElement = TypesAndProperties.fromInstance(instance.getValue());
            ///////////
            System.out.println(instance.getKey() + ": ");
            instance.getValue().forEach(x -> System.out.println("\t" + x));
            System.out.println();
            System.out.println("====>");
            System.out.println("\t>====>");
            System.out.println("\t\t>====>");
            System.out.println();
            System.out.println(schemaElement);

            //////////
            Set<ISchemaElement> schemaElementSet = new HashSet<>();
            schemaElementSet.add(schemaElement);
            if(schemaElements.containsKey(schemaElement.getID())) {
                schemaElements.get(schemaElement.getID()).add(schemaElement);
                System.out.println("Updated element: ("+schemaElement.getID() + ", " +  schemaElements.get(schemaElement.getID()).size() + ")");
            }
            else {
                schemaElements.put(schemaElement.getID(), schemaElementSet);
                System.out.println("Added new element: ("+schemaElement.getID() + ", " +  schemaElements.get(schemaElement.getID()).size() + ")");
            }
            System.out.println("---------------------------");
        }
        for (Map.Entry<Integer, Set<ISchemaElement>> schema : schemaElements.entrySet()){
            System.out.println("____________");
            System.out.println(schema.getKey());
            for(ISchemaElement schemaElement : schema.getValue()){
                System.out.println("\t" + schemaElement);
            }
            System.out.println("____________");

        }

    }
}