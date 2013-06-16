package io.qdb.server.controller;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.DateSerializer;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.google.inject.Inject;
import groovy.lang.GString;
import io.qdb.server.databind.DateTimeParser;
import io.qdb.server.databind.IntegerParser;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Marshaling of objects to/from JSON using Jackson.
 */
@Singleton
public class JsonService {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectMapper mapperNoIdentOutput;

    @Inject
    @SuppressWarnings("deprecation")
    public JsonService(@Named("prettyPrint") boolean prettyPrint) {
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);

        SimpleModule module = new SimpleModule("qdb");

        module.addSerializer(Date.class, new JsonSerializer<Date>(){
            private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"); // ISO 8601
            @Override
            public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
                jgen.writeString(df.format(value));
            }
        });

        module.addDeserializer(Date.class, new JsonDeserializer<Date>() {
            @Override
            public Date deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
                if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) return new Date(jp.getLongValue());
                String s = jp.getText().trim();
                try {
                    return DateTimeParser.INSTANCE.parse(s);
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Invalid date: [" + s + "]");
                }
            }
        });

        JsonDeserializer<Integer> integerJsonDeserializer = new JsonDeserializer<Integer>() {
            @Override
            public Integer deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
                if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) return jp.getIntValue();
                return IntegerParser.INSTANCE.parseInt(jp.getText().trim());
            }
        };
        module.addDeserializer(Integer.class, integerJsonDeserializer);
        module.addDeserializer(Integer.TYPE, integerJsonDeserializer);

        JsonDeserializer<Long> longJsonDeserializer = new JsonDeserializer<Long>() {
            @Override
            public Long deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
                if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) return jp.getLongValue();
                return IntegerParser.INSTANCE.parseLong(jp.getText().trim());
            }
        };
        module.addDeserializer(Long.class, longJsonDeserializer);
        module.addDeserializer(Long.TYPE, longJsonDeserializer);

        mapper.registerModule(module);

        if (prettyPrint) {
            mapperNoIdentOutput = mapper.copy();
            mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        } else {
            mapperNoIdentOutput = mapper;
        }
    }

    /**
     * Convert o to JSON.
     */
    public byte[] toJson(Object o) throws IOException {
        return mapper.writeValueAsBytes(o);
    }

    /**
     * Convert o to JSON with no indenting.
     */
    public byte[] toJsonNoIndenting(Object o) throws IOException {
        return mapperNoIdentOutput.writeValueAsBytes(o);
    }

    /**
     * Convert o to JSON with no indenting.
     */
    public void toJsonNoIndenting(OutputStream out, Object o) throws IOException {
        mapperNoIdentOutput.writeValue(out, o);
    }

    /**
     * Converts content to an instance of a particular type. Throws IllegalArgumentException if JSON is invalid.
     */
    public <T> T fromJson(InputStream ins, Class<T> klass) throws IOException {
        try {
            return mapper.readValue(ins, klass);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }
}
