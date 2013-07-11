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

import org.simpleframework.http.Response;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Date;

@Singleton
public class ServerController extends CrudController {

    private final Date started = new Date();

    public static class StatusDTO {
        public Date started;
        public String qdbVersion;
        public long heapFreeMemory;
        public long heapMaxMemory;
        public long otherFreeMemory;
        public long otherMaxMemory;
    }

    @Inject
    public ServerController(JsonService jsonService) {
        super(jsonService);
    }

    @Override
    protected void list(Call call, int offset, int limit) throws IOException {
        StatusDTO dto = new StatusDTO();
        dto.started = started;

        if (call.getBoolean("gc")) System.gc();
        dto.heapMaxMemory = Runtime.getRuntime().totalMemory();
        dto.heapFreeMemory = Runtime.getRuntime().freeMemory();

        MemoryUsage usage = ManagementFactory.getMemoryMXBean().getNonHeapMemoryUsage();
        dto.otherMaxMemory = usage.getMax();
        dto.otherFreeMemory = dto.otherMaxMemory - usage.getUsed();

        call.setJson(dto);
    }
}
