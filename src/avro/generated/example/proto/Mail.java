package example.proto;

@SuppressWarnings("all")
public interface Mail {
  public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse("{\"protocol\":\"Mail\",\"namespace\":\"example.proto\",\"types\":[{\"type\":\"record\",\"name\":\"Message\",\"fields\":[{\"name\":\"to\",\"type\":\"string\"},{\"name\":\"from\",\"type\":\"string\"},{\"name\":\"body\",\"type\":\"string\"}]}],\"messages\":{\"send\":{\"request\":[{\"name\":\"message\",\"type\":\"Message\"}],\"response\":\"string\"}}}");
  org.apache.avro.util.Utf8 send(example.proto.Message message)
    throws org.apache.avro.ipc.AvroRemoteException;
}
