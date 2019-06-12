package graph;

import junit.framework.TestCase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class EdgeTest extends TestCase {

    private String[] filenames = new String[]{"resources/manual-test-1.nq", "resources/manual-test-2.nq", "resources/manual-test-3.nq"};

    public void testFromNQuad() {
        for (String filename : filenames) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(filename));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            HashMap<String, Set<Edge>> map = new HashMap<>();
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
                    map.merge(edge.start, set, (O, N) -> {
                        O.addAll(N);
                        return O;
                    });
                }
            }

            map.forEach((k, v) -> {
                System.out.println(k + ": ");
                v.forEach(x -> System.out.println("\t" + x));
                System.out.println();
            });
            System.out.println("---------------");
        }


    }
}