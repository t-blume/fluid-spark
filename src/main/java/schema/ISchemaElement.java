package schema;

import graph.Edge;
import scala.Tuple2;

import java.util.Set;

public interface ISchemaElement {


    Set<Tuple2<Edge, Edge>> getSchemaEdges();

    Set<String> getPayload();

    void addPayload(Set<String> payload);
}
