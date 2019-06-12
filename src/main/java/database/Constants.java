package database;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Constants {
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:s");

    public static String NOW(){
        return LocalDateTime.now(ZoneId.of("GMT")).format(DATE_TIME_FORMATTER);
    }


    public static final String RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

    public static final String TYPE = "type";

    public static final String CLASS_SCHEMA_ELEMENT = "SchemaElement";
    public static final String CLASS_SCHEMA_RELATION = "SchemaLink";

    public static final String PROPERTY_SCHEMA_HASH = "hash";
    public static final String PROPERTY_SCHEMA_VALUES = "values";

    public static final int EMPTY_SCHEMA_ELEMENT_HASH = "EMPTY_SCHEMA_ELEMENT_ID".hashCode();


    public static final String PROPERTY_TIMESTAP = "timestamp";

    /**
     * SUper BRain
     */
    public static final String CLASS_IMPRINT_VERTEX = "ImprintVertex";
    public static final String CLASS_IMPRINT_EDGE = "ImprintEdge";

    public static final String PROPERTY_IMPRINT_ID = "ID";
    public static final String CLASS_IMPRINT_RELATION = "imprintLink";


//    public static final String CLASS_SCHEMA = "SchemaLink";
//    public static final String PROPERTY_SE_ID = "ID";
//    public static final String PROPERTY_SE_INSTANCES = "instanceIDs";
//
//    public static final String PROPERTY_EDGE_ID = "ID";
//    public static final String PROPERTY_EDGE_SL = "schemaLinkHashes";
//
//
//    public static final String CLASS_SCHEMA_LINK = "SchemaLink";
//    public static final String PROPERTY_SL_ID = "ID";
//    public static final String PROPERTY_SL_EDGES = "edgeIDs";


    public static void main(String[] args){
        Map<String, Set<String>> map1 = new HashMap<>();
        Map<String, Set<String>> map2 = new HashMap<>();

        Set<String> set1 = new HashSet<>();
        set1.add("value1");
        map1.put("key1", set1);

        Set<String> set2 = new HashSet<>();
        set1.add("value2");

        map2.put("key1", set2);

        for(Map.Entry<String, Set<String>> entry2 : map2.entrySet())
            map1.merge(entry2.getKey(), entry2.getValue(), (O,N) -> {O.addAll(N); return O;});


        map1.entrySet().forEach(E -> System.out.println(E));

    }
}
