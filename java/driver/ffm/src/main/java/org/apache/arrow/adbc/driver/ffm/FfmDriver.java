/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adbc.driver.ffm;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.memory.BufferAllocator;

/** Java 22+ FFM binding to the ADBC C driver manager. */
public final class FfmDriver implements AdbcDriver {
  /** Driver-manager dynamic library path override. */
  public static final String PARAM_MANAGER_PATH = "ffm.manager_path";

  /** Driver shared library or manifest to load. */
  public static final String PARAM_DRIVER = "driver";

  /** Additional driver-manager manifest search paths. */
  public static final String PARAM_MANIFEST_SEARCH_PATH = "ffm.additional_manifest_search_path";

  private final BufferAllocator allocator;

  public FfmDriver(BufferAllocator allocator) {
    this.allocator = Objects.requireNonNull(allocator, "allocator");
  }

  @Override
  public AdbcDatabase open(Map<String, Object> parameters) throws AdbcException {
    Objects.requireNonNull(parameters, "parameters");
    Map<String, String> nativeParameters = new LinkedHashMap<>();
    String additionalSearchPath = null;
    for (Map.Entry<String, Object> parameter : parameters.entrySet()) {
      if (!(parameter.getValue() instanceof String)) {
        throw AdbcException.invalidArgument(
            "[ffm] only String database parameters are supported: " + parameter.getKey());
      }
      String key = parameter.getKey();
      if (key.startsWith("ffm.")) {
        key = key.substring("ffm.".length());
      }
      if (key.equals("manager_path")) {
        System.setProperty(
            "arrow.adbc.driver.ffm.manager.path", (String) parameter.getValue());
        continue;
      }
      if (key.equals("additional_manifest_search_path")) {
        additionalSearchPath = (String) parameter.getValue();
        continue;
      }
      nativeParameters.put(key, (String) parameter.getValue());
    }

    Arena arena = NativeAdbc.newObjectArena();
    MemorySegment database = NativeAdbc.newAdbcObject(arena);
    boolean allocated = false;
    try {
      NativeAdbc.databaseNew(database);
      allocated = true;
      NativeAdbc.setLoadFlags(database);
      if (additionalSearchPath != null) {
        NativeAdbc.setAdditionalSearchPath(database, additionalSearchPath);
      }
      for (Map.Entry<String, String> parameter : nativeParameters.entrySet()) {
        NativeAdbc.databaseSetOption(database, parameter.getKey(), parameter.getValue());
      }
      NativeAdbc.databaseInit(database);
      return new FfmDatabase(allocator, arena, database);
    } catch (AdbcException | RuntimeException e) {
      if (allocated) {
        try {
          NativeAdbc.databaseRelease(database);
        } catch (Exception releaseError) {
          e.addSuppressed(releaseError);
        }
      }
      arena.close();
      if (e instanceof AdbcException) {
        throw (AdbcException) e;
      }
      throw AdbcException.internal("[ffm] failed to initialize database").withCause(e);
    }
  }
}
