package database;

import schema.SchemaElement;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class Constants {
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:s");

    public static String NOW() {
        return LocalDateTime.now(ZoneId.of("GMT")).format(DATE_TIME_FORMATTER);
    }


    //this one is changed by loaded config file
    public static String TYPE = "type";

    public static final String CLASS_SCHEMA_ELEMENT = "SchemaElement";
    public static final String CLASS_SCHEMA_RELATION = "SchemaLink";

    public static final String PROPERTY_SCHEMA_HASH = "hash";
    public static final String PROPERTY_SCHEMA_VALUES = "values";

    public static final String PROPERTY_PAYLOAD = "payload";

    public static final int EMPTY_SCHEMA_ELEMENT_HASH = new SchemaElement().getID();


    public static final String PROPERTY_TIMESTAMP = "timestamp";

    /***************
     * Super Brain *
     ***************/
    public static final String CLASS_IMPRINT_VERTEX = "ImprintVertex";

    public static final String PROPERTY_IMPRINT_ID = "hash";
    public static final String CLASS_IMPRINT_RELATION = "imprintLink";

}
