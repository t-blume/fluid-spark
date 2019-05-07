package database;

import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class OrientDb {


    public static void main(String[] args){
        
    }

    TransactionalGraph graph = new OrientGraph("local:test", "username", "password");
}
