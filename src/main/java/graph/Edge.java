package graph;

import java.io.Serializable;
import java.util.Objects;
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
}
