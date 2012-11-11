package qdb.io.server;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import javax.inject.Singleton;
import java.io.IOException;

/**
 * Marshaling of objects to/from JSON using Jackson.
 */
@Singleton
public class JsonService {

    private ObjectMapper mapper = new ObjectMapper();

    @SuppressWarnings("deprecation")
    public JsonService() {
        mapper.configure(SerializationConfig.Feature.WRITE_NULL_PROPERTIES, false);
        mapper.configure(SerializationConfig.Feature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationConfig.Feature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Convert o to JSON.
     */
    byte[] toJson(Object o) throws IOException {
        return mapper.writeValueAsBytes(o);
    }

    /**
     * Converts content to an instance of a particular type.
     */
    public <T> T fromJson(byte[] content, Class<T> klass) throws IOException {
        return mapper.readValue(content, klass);
    }

}
