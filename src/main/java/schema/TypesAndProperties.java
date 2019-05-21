package schema;


import graph.Edge;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public class TypesAndProperties implements ISchemaElement, Serializable {

    //schema stuff
    private Set<String> labels;
    private Set<Tuple2<Edge, Edge>> schemaEdges;
    //payload stuff
    private Set<String> payload;

    public TypesAndProperties() {
        labels = new HashSet<>();
        schemaEdges = new HashSet<>();
        payload = new HashSet<>();


    }

    public static ISchemaElement fromInstance(Set<Edge> edges){
        TypesAndProperties schemaElement = new TypesAndProperties();
        for(Edge edge : edges){
            if(edge.label.trim().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")){
                schemaElement.labels.add(edge.end);
            }else {
                //simple stuff here, only use label
                Edge schemaEdge = new Edge();
                schemaEdge.label = edge.label;
                schemaEdge.end = null;
                schemaElement.schemaEdges.add(new Tuple2<>(edge, schemaEdge));
            }
            //add source as payload
            schemaElement.payload.add(edge.source);
        }
        return schemaElement;
    }


    @Override
    public int getID() {
        Set<Edge> edges = new HashSet<>();
        schemaEdges.forEach(T -> edges.add(T._2));
        return 17 + labels.hashCode() + 31 + edges.hashCode();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypesAndProperties that = (TypesAndProperties) o;
        return Objects.equals(getID(), that.getID());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getID());
    }

    @Override
    public String toString() {
        String string = "id: " + getID() + "\nlabels=[";

        Iterator<String> iterator = labels.iterator();
        while (iterator.hasNext())
            string += iterator.next() + ", ";


        string += "]\nedges:\n";
        for (Tuple2<Edge, Edge> tuple : schemaEdges)
            string += "\t" + tuple._2.label + "\n";

        string += "\nsources=[";

        iterator = payload.iterator();
        while (iterator.hasNext())
            string += iterator.next() + ", ";
        string += "]";
        return string;
    }

    @Override
    public Set<String> getLabel() {
        return labels;
    }

    @Override
    public Set<Tuple2<Edge, Edge>> getSchemaEdges() {
        return schemaEdges;
    }

    @Override
    public Set<String> getPayload() {
        return payload;
    }

    @Override
    public void addPayload(Set<String> payload) {
        this.payload.addAll(payload);
    }
}
