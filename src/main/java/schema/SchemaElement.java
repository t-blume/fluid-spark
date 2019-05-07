package schema;

import graph.Edge;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class SchemaElement implements Serializable {
    public int id;
    public Set<String> labels;
    public Set<Edge> edges;

    public Set<String> sources;

    public SchemaElement() {
        labels = new HashSet<>();
        edges = new HashSet<>();
        id = 0;
        sources = new HashSet<>();

    }

    public static SchemaElement fromInstance(Set<Edge> edges){
        SchemaElement schemaElement = new SchemaElement();

        for(Edge edge : edges){
            if(edge.label.trim().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")){
                schemaElement.labels.add(edge.end);
            }else {
                //simple stuff here, only use label
                Edge schemaEdge = new Edge();
                schemaEdge.label = edge.label;
                schemaElement.edges.add(schemaEdge);
            }
            schemaElement.sources.add(edge.source);
        }
        schemaElement.updateID();
        return schemaElement;
    }

    public void updateID() {
        id = 17 + labels.hashCode() + 31 + edges.hashCode();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaElement that = (SchemaElement) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public String toString() {
        String string = "id: " + id + "\nlabels=[";

        Iterator<String> iterator = labels.iterator();
        while (iterator.hasNext())
            string += iterator.next() + ", ";


        string += "]\nedges:\n";
        for (Edge edge : edges)
            string += "\t" + edge.label + "\n";

        string += "\nsources=[";

        iterator = sources.iterator();
        while (iterator.hasNext())
            string += iterator.next() + ", ";
        string += "]";
        return string;
    }

}
