package graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Edge implements Serializable {

    public String start;
    public String end;
    public String label;
    public String source;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Edge edge = (Edge) o;
        return Objects.equals(start, edge.start) &&
                Objects.equals(end, edge.end) &&
                Objects.equals(label, edge.label) &&
                Objects.equals(source, edge.source);
    }

    @Override
    public int hashCode() {
        return Objects.hash(start, end, label, source);
    }

    public String toString() {
        return start + " --- " + label + " ---> " + end + " [" + source + "]";
    }

    public static Edge fromNQuad(String string) {

        Pattern p = Pattern.compile("^(<.*>|_:.*) (<.*>) (<.*>|\".*\"(@.*|\\^\\^<.*>)?|_:.*) (<.*>) \\.$");
        Matcher m = p.matcher(string);

        if (m.matches()) {
            Edge edge = new Edge();

            edge.start = m.group(1).replaceAll("<|>", "");
            edge.label = m.group(2).replaceAll("<|>", "");
            edge.end = m.group(3).replaceAll("<|>", "");

            edge.source = m.group(5).replaceAll("<|>", "");

            return edge;
        } else
            return null;

    }


    public static void main(String[] args) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("resources/timbl-500.nq"));

        HashMap<String, Set<Edge>> map = new HashMap<>();

        String line;
        while ((line = reader.readLine()) != null) {
            Edge edge = Edge.fromNQuad(line);
            if (edge != null) {
                Set<Edge> set = new HashSet<>();
                set.add(edge);
                map.merge(edge.start, set, (O,N) -> {
                    O.addAll(N);
                    return O;
                });
            }
        }

        map.forEach((k,v) -> {
            System.out.println(k + ": ");
            v.forEach(x -> System.out.println("\t" + x));
            System.out.println();
        });
    }
}
