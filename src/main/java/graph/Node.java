package graph;

import java.util.Objects;
import java.util.Set;

public class Node {
    public String id;
    public Set<String> labels;
    public String source;



    public static Node fromNQuad(String string) {
        return null;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(id, node.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        String string = id + "{";
        if (labels != null) {
            for (int i = 0; i < labels.size() - 1; i++)
                string += labels.iterator().next() + ", ";
            string += labels.iterator().next();
        }
        string += "} [" + source + "]";

        return string;
    }
}
