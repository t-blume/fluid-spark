package database;

import schema.VertexSummary;

public class Constants {
    public static final String ALL_LABEL = "*";

    public static final String CLASS_SUMMARY_VERTEX = "SummaryVertex";
    public static final String CLASS_SUMMARY_RELATION = "SummaryEdge";

    public static final String PROPERTY_SCHEMA_HASH = "hash";
    public static final String PROPERTY_SCHEMA_VALUES = "values";

    public static final String PROPERTY_PAYLOAD = "payload";


    public static final int EMPTY_VERTEX_SUMMARY_HASH = new VertexSummary().getID();

}
