package schema;


import graph.Edge;
import scala.Serializable;
import scala.Tuple2;

import java.util.Set;

public interface ISchemaElement extends Serializable {

    Set<String> getLabel();

    void setLabel(Set<String> label);

    Set<Tuple2<Edge, Edge>> getSchemaEdges();

    Set<String> getPayload();

    void addPayload(Set<String> payload);

    int getID();

    Set<String> getDependencies();


    void merge(ISchemaElement schemaElement);

//    static ISchemaElementfromInstance(Set<Edge> edges);
}
