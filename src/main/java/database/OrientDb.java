package database;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OrientDb {


    public static void main(String[] args){
        TransactionalGraph graph = new OrientGraph("remote:localhost/test", "admin", "admin");
        System.out.println("asd");
        try {
           graph.getVertices().forEach(V ->  System.out.println(V));
            System.out.println(".....");
            OrientVertex orientVertex = new OrientVertex();
            System.out.println(orientVertex.getIdentity());
            orientVertex.setProperty("@rid", "#1:1234");
            System.out.println("---->");

            System.out.println(orientVertex.getIdentity());

            Vertex objectCluster = graph.addVertex("class:OC");
            objectCluster.setProperty("object", "bibo:Book");
            objectCluster.setProperty("object", "dc:Document");
            objectCluster.setProperty("hash", 123);

            Vertex vPerson = graph.addVertex("class:EQC");
            vPerson.setProperty("object", "bibo:Book");
            vPerson.setProperty("object", "dc:Document");

            graph.commit();
//            Edge edge = graph.addEdge(vPerson.getId(), )
        } finally {
            graph.shutdown();
        }


    }

}
