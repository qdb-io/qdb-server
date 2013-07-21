/*
 * Copyright 2013 David Tinker
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.qdb.server.controller;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;
import com.fasterxml.jackson.databind.ser.std.NumberSerializers;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.inject.Inject;
import humanize.Humanize;
import io.qdb.server.databind.DateTimeParser;
import io.qdb.server.databind.IntegerParser;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Marshaling of objects to/from JSON using Jackson for the REST API.
 */
@Singleton
public class JsonService {

    private final ObjectMapper mapper;
    private final ObjectMapper mapperMsgHeader;
    private final ObjectMapper mapperBorg;
    private final ObjectMapper mapperBorgMsgHeader;

    private final JsonDeserializer<Long> longJsonDeserializer = new JsonDeserializer<Long>() {
        @Override
        public Long deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) return jp.getLongValue();
            return IntegerParser.INSTANCE.parseLong(jp.getText().trim());
        }
    };

    private final JsonDeserializer<Integer> integerJsonDeserializer = new JsonDeserializer<Integer>() {
        @Override
        public Integer deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            if (jp.getCurrentToken() == JsonToken.VALUE_NUMBER_INT) return jp.getIntValue();
            return IntegerParser.INSTANCE.parseInt(jp.getText().trim());
        }
    };

    private final JsonDeserializer<Date> dateDeserializer = new JsonDeserializer<Date>() {
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
    };

    BorgOrHumanSerializer<Integer> integerSerializer = new BorgOrHumanSerializer<Integer>(Integer.TYPE,
            new HumanNumberSerializer<Integer>(Integer.TYPE),
            new NumberSerializers.IntegerSerializer());

    BorgOrHumanSerializer<Long> longSerializer = new BorgOrHumanSerializer<Long>(Long.TYPE,
            new HumanNumberSerializer<Long>(Long.TYPE),
            new NumberSerializers.LongSerializer());

    @Inject
    @SuppressWarnings("deprecation")
    public JsonService(@Named("prettyPrint") boolean prettyPrint) {
        mapper = createMapper(prettyPrint, false, false);
        mapperMsgHeader = createMapper(false, false, true);
        mapperBorg = createMapper(prettyPrint, true, true);
        mapperBorgMsgHeader = prettyPrint ? createMapper(false, true, true) : mapperBorg;
    }

    private ObjectMapper createMapper(boolean prettyPrint, boolean datesAsTimestamps, boolean borg) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        mapper.configure(SerializationFeature.INDENT_OUTPUT, prettyPrint);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, datesAsTimestamps);

        SimpleModule module = new SimpleModule();
        module.addDeserializer(Date.class, dateDeserializer);
        module.addDeserializer(Integer.class, integerJsonDeserializer);
        module.addDeserializer(Integer.TYPE, integerJsonDeserializer);
        module.addDeserializer(Long.class, longJsonDeserializer);
        module.addDeserializer(Long.TYPE, longJsonDeserializer);
        if (!borg) {
            module.addSerializer(Integer.TYPE, integerSerializer);
            module.addSerializer(Integer.class, integerSerializer);
            module.addSerializer(Long.TYPE, longSerializer);
            module.addSerializer(Long.class, longSerializer);
        }
        if (!datesAsTimestamps) module.addSerializer(Date.class, new ISO8601DateSerializer());
        mapper.registerModule(module);

        return mapper;
    }

    /**
     * Convert o to JSON.
     */
    public byte[] toJson(Object o, boolean borg) throws IOException {
        return (borg ? mapperBorg : mapper).writeValueAsBytes(o);
    }

    /**
     * Convert o to JSON with no indenting and no humanization of sizes.
     */
    public byte[] toJsonMsgHeader(Object o, boolean borg) throws IOException {
        return (borg ? mapperBorgMsgHeader :  mapperMsgHeader).writeValueAsBytes(o);
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

    /**
     * Chooses between human and borg serializers based on the name of the property being serialized.
     */
    private static class BorgOrHumanSerializer<T> extends StdSerializer<T> implements ContextualSerializer {

        private final JsonSerializer<T> human;
        private final JsonSerializer<T> borg;

        public BorgOrHumanSerializer(Class<T> t, JsonSerializer<T> human, JsonSerializer<T> borg) {
            super(t);
            this.human = human;
            this.borg = borg;
        }

        @Override
        public JsonSerializer<T> createContextual(SerializerProvider prov, BeanProperty property) throws JsonMappingException {
            if (property != null) {
                String name = property.getName();
                if (name.endsWith("emory") || name.endsWith("ize") || name.endsWith("ytes")) return human;
            }
            return borg;
        }

        @Override
        public void serialize(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            borg.serialize(value, jgen, provider);
        }
    }

    /**
     * Serializers numbers as strings in human form e.g. 1 GB.
     */
    private static class HumanNumberSerializer<T extends Number> extends StdScalarSerializer<T> {

        public HumanNumberSerializer(Class<T> t) {
            super(t);
        }

        @Override
        public void serialize(T value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeString(Humanize.binaryPrefix(value));
        }
    }

    private static class ISO8601DateSerializer extends JsonSerializer<Date> {

        private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); // ISO 8601

        @Override
        public void serialize(Date value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
            jgen.writeString(df.format(value));
        }
    }
}
