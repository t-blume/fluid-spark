package schema;

import graph.Edge;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import static database.Constants.TYPE;


public class SchemEX implements ISchemaElement, Serializable {

    //schema stuff
    private Set<String> labels;
    private Set<Tuple2<Edge, Edge>> schemaEdges;
    //payload stuff
    private Set<String> payload;

    public SchemEX() {
        labels = new HashSet<>();
        schemaEdges = new HashSet<>();
        payload = new HashSet<>();
    }


    public static ISchemaElement fromInstance(Set<Edge> edges) {
        SchemEX schemaElement = new SchemEX();
        for (Edge edge : edges) {
            //extract the types
            if (edge.label.trim().equals(TYPE))
                schemaElement.labels.add(edge.end);
            else {
                //the properties
                Edge schemaEdge = new Edge();
                schemaEdge.label = edge.label;
                //create a ref to the neighbor
                schemaEdge.end = edge.end;
                schemaElement.schemaEdges.add(new Tuple2<>(edge, schemaEdge));
            }
            //add source as payload
            schemaElement.payload.add(edge.source);
        }
        return schemaElement;
    }


    public static ISchemaElement combine(ISchemaElement schemaElement, Set<ISchemaElement> neighborElements) {
        return schemaElement; //TODO
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
    public int getID() {
        Set<Edge> edges = new HashSet<>();
        schemaEdges.forEach(T -> edges.add(T._2));
        return 17 + labels.hashCode() + 31 + edges.hashCode();
    }

    @Override
    public Set<String> getDependencies() {
        Set<String> dependencies = new HashSet<>();
        for (Tuple2<Edge, Edge> tuple2 : schemaEdges) {
            String tmp = tuple2._2.end;
            if (tmp != null)
                dependencies.add(tmp);
        }
        return dependencies;
    }

    @Override
    public void merge(ISchemaElement schemaElement) {
        this.labels.addAll(schemaElement.getLabel());
        this.schemaEdges.addAll(schemaElement.getSchemaEdges());
        this.payload.addAll(schemaElement.getPayload());
    }


    @Override
    public String toString() {
        return "SchemEX{" +
                "labels=" + labels +
                ", schemaEdges=" + schemaEdges +
                ", payload=" + payload +
                '}';
    }
}
