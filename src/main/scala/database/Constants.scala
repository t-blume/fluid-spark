package database

import java.time.{LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter

import schema.SchemaElement

object Constants {
  val DATE_TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:s")

  def NOW: String = LocalDateTime.now(ZoneId.of("GMT")).format(DATE_TIME_FORMATTER)


  val CLASS_SCHEMA_ELEMENT = "SchemaElement"
  val CLASS_SCHEMA_RELATION = "SchemaLink"

  val PROPERTY_SCHEMA_HASH = "hash"
  val PROPERTY_SCHEMA_VALUES = "values"

  //    public static final String PROPERTY_SUMMARIZED_INSTANCES = "instances";
  //    public static final String PROPERTY_PAYLOAD = "payload";

  val EMPTY_SCHEMA_ELEMENT_HASH: Int = new SchemaElement().getID
}
