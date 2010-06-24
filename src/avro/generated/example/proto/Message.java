package example.proto;

@SuppressWarnings("all")
public class Message extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Message\",\"namespace\":\"example.proto\",\"fields\":[{\"name\":\"to\",\"type\":\"string\"},{\"name\":\"from\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}");
  public org.apache.avro.util.Utf8 to;
  public org.apache.avro.util.Utf8 from;
  public org.apache.avro.util.Utf8 body;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return to;
    case 1: return from;
    case 2: return body;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: to = (org.apache.avro.util.Utf8)value$; break;
    case 1: from = (org.apache.avro.util.Utf8)value$; break;
    case 2: body = (org.apache.avro.util.Utf8)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
