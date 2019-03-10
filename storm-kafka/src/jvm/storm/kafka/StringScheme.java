package storm.kafka;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class StringScheme implements Scheme {

    private String outputFieldName;

    public StringScheme() {
        this("str");
    }

    public StringScheme(String outputFieldName) {
        this.outputFieldName = outputFieldName;

    }

    public List<Object> deserialize(byte[] bytes) {
        try {
            return new Values(new String(bytes, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
        return new Fields(outputFieldName);
    }
}

