package io.qdb.server.repo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Converts repository related objects to/from JSON.
 */
@Singleton
public class JsonConverter {

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonConverter() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
    }

    public <T> T readValue(InputStream src, Class<T> valueType) throws IOException {
        return mapper.readValue(src, valueType);
    }

    public <T> T readValue(byte[] src, Class<T> valueType) throws IOException {
        return mapper.readValue(src, valueType);
    }

    public byte[] writeValueAsBytes(Object value) throws IOException {
        return mapper.writeValueAsBytes(value);
    }

    public void writeValue(OutputStream out, Object value) throws IOException {
        mapper.writeValue(out, value);
    }

}
