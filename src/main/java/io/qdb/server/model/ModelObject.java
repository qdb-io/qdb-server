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

package io.qdb.server.model;

import com.rits.cloning.Cloner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for objects in our model. Supports equals (class and id must match) and hashcode (on id).
 * Serializable to/from JSON with Genson. All subclasses have a deepCopy method.
 */
public abstract class ModelObject implements Comparable<ModelObject>, Cloneable {

    private String id;
    private int version;

    protected static final Cloner CLONER = new Cloner();

    protected ModelObject() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public void incVersion() {
        ++version;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return o != null && o.getClass().isAssignableFrom(getClass()) && id.equals(((ModelObject)o).id);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + id;
    }

    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    @Override
    protected Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e.toString(), e);  // not possible since we are Cloneable
        }
    }

    @Override
    public int compareTo(ModelObject o) {
        return id.compareTo(o.id);
    }

    /**
     * Return this objects properties in a map. If the object has a params property then these are included in the
     * map and not nested.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> toMap() {
        HashMap<String, Object> ans = new HashMap<String, Object>();
        for (Method m : getClass().getMethods()) {
            String name = m.getName();
            if (!name.startsWith("get") || m.getParameterTypes().length > 0) continue;
            name = Character.toLowerCase(name.charAt(3)) + name.substring(4);
            Object v;
            try {
                v = m.invoke(this);
            } catch (Exception e) { // this shouldn't happen
                throw new IllegalArgumentException(e.toString(), e);
            }
            if (v != null) {
                if ("params".equals(name)) ans.putAll((Map)v);
                else ans.put(name, v);
            }
        }
        return ans;
    }
}
