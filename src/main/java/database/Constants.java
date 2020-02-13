package database;

import schema.SchemaElement;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Constants {
    public static final String ALL_LABEL = "*";

    public static final String CLASS_SCHEMA_ELEMENT = "SchemaElement";
    public static final String CLASS_SCHEMA_RELATION = "SchemaLink";

    public static final String PROPERTY_SCHEMA_HASH = "hash";
    public static final String PROPERTY_SCHEMA_VALUES = "values";

    public static final int EMPTY_SCHEMA_ELEMENT_HASH = new SchemaElement().getID();

}
