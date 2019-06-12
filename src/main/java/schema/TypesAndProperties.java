package schema;


import graph.Edge;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

import static database.Constants.TYPE;

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
            if(edge.label.trim().equals(TYPE)){
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
        Set<String> properties = new HashSet<>();
        schemaEdges.forEach(T -> properties.add(T._2.label));
        return 17 + labels.hashCode() + 31 + properties.hashCode();
    }

    @Override
    public Set<String> getDependencies() {
        return null;
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
        return Objects.hash(labels, schemaEdges, payload);
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
        string += "hash: " + hashCode();
        return string;
    }

    @Override
    public Set<String> getLabel() {
        return labels;
    }

    @Override
    public void setLabel(Set<String> label) {
        this.labels = label;
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

    @Override
    public void merge(ISchemaElement schemaElement) {
        this.labels.addAll(schemaElement.getLabel());
        this.schemaEdges.addAll(schemaElement.getSchemaEdges());
        this.payload.addAll(schemaElement.getPayload());
    }
}
