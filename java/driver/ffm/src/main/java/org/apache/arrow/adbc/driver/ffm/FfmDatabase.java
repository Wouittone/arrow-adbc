/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.memory.BufferAllocator;

final class FfmDatabase implements AdbcDatabase {
  private final BufferAllocator allocator;
  private final Arena arena;
  private final MemorySegment handle;
  private final Set<FfmConnection> connections = new LinkedHashSet<>();
  private boolean closed;

  FfmDatabase(BufferAllocator allocator, Arena arena, MemorySegment handle) {
    this.allocator = allocator;
    this.arena = arena;
    this.handle = handle;
  }

  @Override
  public synchronized AdbcConnection connect() throws AdbcException {
    ensureOpen();
    Arena connectionArena = NativeAdbc.newObjectArena();
    MemorySegment connection = NativeAdbc.newAdbcObject(connectionArena);
    boolean allocated = false;
    try {
      NativeAdbc.connectionNew(connection);
      allocated = true;
      NativeAdbc.connectionInit(connection, handle);
      FfmConnection result = new FfmConnection(allocator, this, connectionArena, connection);
      connections.add(result);
      return result;
    } catch (AdbcException | RuntimeException e) {
      if (allocated) {
        try {
          NativeAdbc.connectionRelease(connection);
        } catch (Exception releaseError) {
          e.addSuppressed(releaseError);
        }
      }
      connectionArena.close();
      if (e instanceof AdbcException) {
        throw (AdbcException) e;
      }
      throw AdbcException.internal("[ffm] failed to initialize connection").withCause(e);
    }
  }

  synchronized void release(FfmConnection connection) {
    connections.remove(connection);
  }

  @Override
  public void close() throws AdbcException {
    ArrayList<FfmConnection> openConnections;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      openConnections = new ArrayList<>(connections);
    }
    AdbcException failure = null;
    for (FfmConnection connection : openConnections) {
      try {
        connection.close();
      } catch (AdbcException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    try {
      NativeAdbc.databaseRelease(handle);
    } catch (AdbcException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    } finally {
      arena.close();
    }
    if (failure != null) {
      throw failure;
    }
  }

  @Override
  public synchronized <T> T getOption(TypedKey<T> key) throws AdbcException {
    ensureOpen();
    return FfmOptions.get("Database", handle, key);
  }

  @Override
  public synchronized <T> void setOption(TypedKey<T> key, T value) throws AdbcException {
    ensureOpen();
    FfmOptions.set("Database", handle, key, value);
  }

  synchronized void ensureOpen() throws AdbcException {
    if (closed) {
      throw AdbcException.invalidState("[ffm] database is closed");
    }
  }
}
