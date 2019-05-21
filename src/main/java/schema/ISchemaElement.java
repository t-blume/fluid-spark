package schema;



import graph.Edge;
import scala.Serializable;
import scala.Tuple2;

import java.util.Set;

public interface ISchemaElement extends Serializable {

    Set<String> getLabel();

    Set<Tuple2<Edge, Edge>> getSchemaEdges();

    Set<String> getPayload();

    void addPayload(Set<String> payload);

    int getID();


//    static ISchemaElementfromInstance(Set<Edge> edges);
}
