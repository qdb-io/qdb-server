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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Marshaling of objects to/from JSON using Jackson.
 */
@Singleton
public class JsonService {

    private static final Logger log = LoggerFactory.getLogger(JsonService.class);

    private final ObjectMapper mapper;
    private final ObjectMapper mapperNoIdentOutput;
    private final ObjectMapper mapperBorg;

    @Inject
    @SuppressWarnings("deprecation")
    public JsonService(@Named("prettyPrint") boolean prettyPrint) {
        SimpleModule qdbModule = createQdbModule();

        mapperNoIdentOutput = createMapper(qdbModule, false);
        mapper = createMapper(qdbModule, prettyPrint);
        mapper.registerModule(createHumanModule());

        mapperBorg = createMapper(qdbModule, prettyPrint);
    }

    private ObjectMapper createMapper(Module module, boolean indent) {
        ObjectMapper m = new ObjectMapper();
        m.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        m.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        m.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
        if (indent) m.configure(SerializationFeature.INDENT_OUTPUT, true);
        m.registerModule(module);
        return m;
    }

    private SimpleModule createQdbModule() {
        SimpleModule module = new SimpleModule("qdb");

        module.addSerializer(Date.class, new JsonSerializer<Date>() {
            private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); // ISO 8601

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
        return module;
    }

    private SimpleModule createHumanModule() {
        SimpleModule module = new SimpleModule("qdb-human");

        BorgOrHumanSerializer<Long> longSer = new BorgOrHumanSerializer<Long>(Long.TYPE,
                new HumanNumberSerializer<Long>(Long.TYPE),
                new NumberSerializers.LongSerializer());
        module.addSerializer(Long.TYPE, longSer);
        module.addSerializer(Long.class, longSer);

        BorgOrHumanSerializer<Integer> integerSer = new BorgOrHumanSerializer<Integer>(Integer.TYPE,
                new HumanNumberSerializer<Integer>(Integer.TYPE),
                new NumberSerializers.IntegerSerializer());
        module.addSerializer(Integer.TYPE, integerSer);
        module.addSerializer(Integer.class, integerSer);

        return module;
    }

    /**
     * Convert o to JSON.
     */
    public byte[] toJson(Object o, boolean borg) throws IOException {
        return borg ? mapperBorg.writeValueAsBytes(o) : mapper.writeValueAsBytes(o);
    }

    /**
     * Convert o to JSON with no indenting.
     */
    public byte[] toJsonNoIndenting(Object o) throws IOException {
        return mapperNoIdentOutput.writeValueAsBytes(o);
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
                if (name.endsWith("Memory") || name.endsWith("Size")) return human;
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
}
