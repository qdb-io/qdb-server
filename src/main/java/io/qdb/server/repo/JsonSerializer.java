package io.qdb.server.repo;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.qdb.kvstore.KeyValueStore;

import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Converts repository related objects to/from JSON.
 */
@Singleton
public class JsonSerializer {

    private final ObjectMapper mapper = new ObjectMapper();

    public JsonSerializer() {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
        mapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT, JsonTypeInfo.As.PROPERTY);

//        TypeResolverBuilder<?> typer = new ObjectMapper.DefaultTypeResolverBuilder(
//                ObjectMapper.DefaultTyping.JAVA_LANG_OBJECT);
//        typer = typer.init(JsonTypeInfo.Id.CLASS, null);
//        typer = typer.inclusion(JsonTypeInfo.As.PROPERTY);
//        mapper.setDefaultTyping(typer);
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

    public void serialize(Object value, OutputStream out) throws IOException {
        mapper.writeValue(out, value);
    }

    public <T> T deserialize(InputStream in, Class<T> cls) throws IOException {
        return mapper.readValue(in, cls);
    }
}
