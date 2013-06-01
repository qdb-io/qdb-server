package io.qdb.server.controller;

import com.fasterxml.jackson.core.JsonParseException;
import io.qdb.kvstore.KeyValueStoreException;
import org.simpleframework.http.Form;
import org.simpleframework.http.Request;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.channels.Channels;
import java.util.Map;

/**
 * Base class for controllers that provide CRUD for some resource.
 */
public abstract class CrudController implements Controller {

    protected final JsonService jsonService;

    protected CrudController(JsonService jsonService) {
        this.jsonService = jsonService;
    }

    public void handle(Call call) throws IOException {
        try {
            String id = call.nextSegment();
            if (id == null) {
                if (call.isGet()) {
                    if (call.getBoolean("count")) {
                        count(call);
                    } else {
                        int offset = call.getInt("offset", 0);
                        list(call, offset, call.getInt("limit", Integer.MAX_VALUE - offset));
                    }
                } else if (call.isPost()) {
                    create(call);
                } else {
                    call.setCode(400);
                }
            } else {
                String resource = call.nextSegment();
                if (resource == null) {
                    if (call.isGet()) show(call, id);
                    else if (call.isPost() || call.isPut()) createOrUpdate(call, id);
                    else if (call.isDelete()) delete(call, id);
                    else call.setCode(400);
                } else {
                    getController(call, id, resource).handle(call);
                }
            }
        } catch (BadRequestException e) {
            call.setCode(400, e.getMessage());
        } catch (KeyValueStoreException e) {
            call.setCode(400, e.getMessage());
        }
    }

    protected void list(Call call, int offset, int limit) throws IOException {
        call.setCode(404);
    }

    protected void count(Call call) throws IOException {
        call.setCode(400, "Count not supported");
    }

    protected void show(Call call, String id) throws IOException {
        call.setCode(404);
    }

    protected void create(Call call) throws IOException {
        call.setCode(400, "Create not supported");
    }

    protected void createOrUpdate(Call call, String id) throws IOException {
        call.setCode(400, "Create not supported");
    }

    protected void update(Call call, String id) throws IOException {
        call.setCode(400, "Update not supported");
    }

    protected void delete(Call call, String id) throws IOException {
        call.setCode(400, "Delete not supported");
    }

    protected Controller getController(Call call, String id, String resource) throws IOException {
        return StatusCodeController.SC_404;
    }

    /**
     * Create an instance of cls from the body (if it is JSON) or the form parameters otherwise.
     * Throws BadRequestException for invalid fields or JSON.
     */
    protected <T> T getBodyObject(Call call, Class<T> cls) throws IOException {
        Request req = call.getRequest();
        if ("application/json".equals(req.getValue("Content-Type"))) {
            InputStream ins = req.getInputStream();
            try {
                return jsonService.fromJson(ins, cls);
            } catch (IllegalArgumentException x) {
                throw new BadRequestException(x.getMessage());
            } finally {
                ins.close();
            }
        } else {    // assume form data and populate instance with params that match field names of the dto
            T dto;
            try {
                dto = cls.newInstance();
            } catch (Exception x) {
                throw new RuntimeException(x.toString(), x);
            }
            Form form = req.getForm();
            for (Map.Entry<String, String> e : form.entrySet()) {
                Field f;
                try {
                    f = cls.getField(e.getKey());
                } catch (NoSuchFieldException x) {
                    throw new BadRequestException("Unknown field: " + e.getKey() + "=" + e.getValue());
                }
                Class<?> t = f.getType();
                Object v = e.getValue();
                try {
                    if (t == Integer.TYPE || t == Integer.class) v = Integer.parseInt((String) v);
                    else if (t == Long.TYPE || t == Long.class) v = Long.parseLong((String) v);
                    else if (t == Boolean.TYPE || t == Boolean.class) v = "true".equals(v);
                    else if (t != String.class) continue;
                    f.set(dto, v);
                } catch (Exception x) {
                    throw new BadRequestException("Invalid field value, expected " + t.getSimpleName() + ": " +
                            e.getKey() + "=" + e.getValue());
                }
            }
            return dto;
        }
    }

    public static class Count {

        public int count;

        public Count(int count) {
            this.count = count;
        }
    }
}
