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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

final class FfmStatement implements AdbcStatement {
  private final BufferAllocator allocator;
  private final FfmConnection parent;
  private final Arena arena;
  private final MemorySegment handle;
  private final Set<ArrowReader> readers = new LinkedHashSet<>();
  private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
  private VectorSchemaRoot bindRoot;
  private ArrowReader bindReader;
  private boolean closed;

  FfmStatement(
      BufferAllocator allocator, FfmConnection parent, Arena arena, MemorySegment handle) {
    this.allocator = allocator;
    this.parent = parent;
    this.arena = arena;
    this.handle = handle;
  }

  @Override
  public void cancel() throws AdbcException {
    lifecycleLock.readLock().lock();
    try {
      ensureOpen();
      NativeAdbc.statementCancel(handle);
    } finally {
      lifecycleLock.readLock().unlock();
    }
  }

  @Override
  public synchronized void setSqlQuery(String query) throws AdbcException {
    ensureOpen();
    NativeAdbc.statementSetSqlQuery(handle, query);
  }

  @Override
  public synchronized void setSubstraitPlan(ByteBuffer plan) throws AdbcException {
    ensureOpen();
    ByteBuffer input = plan.duplicate();
    byte[] bytes = new byte[input.remaining()];
    input.get(bytes);
    try (Arena scratch = Arena.ofConfined()) {
      MemorySegment data = scratch.allocate(bytes.length, 1);
      data.copyFrom(MemorySegment.ofArray(bytes));
      NativeAdbc.statementSetSubstraitPlan(handle, data, bytes.length);
    }
  }

  @Override
  public synchronized void bind(VectorSchemaRoot root) throws AdbcException {
    ensureOpen();
    clearBindReader();
    bindRoot = root;
  }

  @Override
  public synchronized void bind(ArrowReader reader) throws AdbcException {
    ensureOpen();
    clearBindReader();
    bindRoot = null;
    bindReader = reader;
  }

  private void clearBindReader() throws AdbcException {
    if (bindReader != null) {
      try {
        bindReader.close();
      } catch (IOException e) {
        throw AdbcException.internal("[ffm] failed to close previous bind reader").withCause(e);
      } finally {
        bindReader = null;
      }
    }
  }

  private void exportBind() throws AdbcException {
    if (bindRoot != null) {
      try (ArrowArray batch = ArrowArray.allocateNew(allocator);
          ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
        Data.exportSchema(allocator, bindRoot.getSchema(), null, schema);
        Data.exportVectorSchemaRoot(allocator, bindRoot, null, batch);
        NativeAdbc.statementBind(handle, batch.memoryAddress(), schema.memoryAddress());
      }
    } else if (bindReader != null) {
      try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
        Data.exportArrayStream(allocator, bindReader, stream);
        NativeAdbc.statementBindStream(handle, stream.memoryAddress());
        bindReader = null;
      }
    }
  }

  @Override
  public synchronized QueryResult executeQuery() throws AdbcException {
    ensureOpen();
    exportBind();
    try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      long rowsAffected = NativeAdbc.statementExecuteQuery(handle, stream.memoryAddress());
      ArrowReader reader = Data.importArrayStream(allocator, stream);
      readers.add(reader);
      return new QueryResult(rowsAffected, reader);
    }
  }

  @Override
  public synchronized UpdateResult executeUpdate() throws AdbcException {
    try (QueryResult result = executeQuery()) {
      return new UpdateResult(result.getAffectedRows());
    } catch (IOException e) {
      throw AdbcException.internal("[ffm] failed to close update result stream").withCause(e);
    }
  }

  @Override
  public synchronized Schema executeSchema() throws AdbcException {
    ensureOpen();
    exportBind();
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      NativeAdbc.statementExecuteSchema(handle, schema.memoryAddress());
      return Data.importSchema(allocator, schema);
    }
  }

  @Override
  public synchronized Schema getParameterSchema() throws AdbcException {
    ensureOpen();
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      NativeAdbc.statementGetParameterSchema(handle, schema.memoryAddress());
      return Data.importSchema(allocator, schema);
    }
  }

  @Override
  public synchronized void prepare() throws AdbcException {
    ensureOpen();
    NativeAdbc.statementPrepare(handle);
  }

  @Override
  public synchronized <T> T getOption(TypedKey<T> key) throws AdbcException {
    ensureOpen();
    return FfmOptions.get("Statement", handle, key);
  }

  @Override
  public synchronized <T> void setOption(TypedKey<T> key, T value) throws AdbcException {
    ensureOpen();
    FfmOptions.set("Statement", handle, key, value);
  }

  synchronized void ensureOpen() throws AdbcException {
    parent.ensureOpen();
    if (closed) {
      throw AdbcException.invalidState("[ffm] statement is closed");
    }
  }

  @Override
  public void close() throws AdbcException {
    ArrowReader inputReader;
    ArrayList<ArrowReader> openReaders;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      inputReader = bindReader;
      bindReader = null;
      bindRoot = null;
      openReaders = new ArrayList<>(readers);
    }
    AdbcException failure = null;
    if (inputReader != null) {
      try {
        inputReader.close();
      } catch (IOException e) {
        failure = AdbcException.internal("[ffm] failed to close bind reader").withCause(e);
      }
    }
    for (ArrowReader reader : openReaders) {
      try {
        reader.close();
      } catch (IOException e) {
        AdbcException closeError =
            AdbcException.internal("[ffm] failed to close query result stream").withCause(e);
        if (failure == null) {
          failure = closeError;
        } else {
          failure.addSuppressed(closeError);
        }
      }
    }
    synchronized (this) {
      readers.clear();
    }
    lifecycleLock.writeLock().lock();
    try {
      NativeAdbc.statementRelease(handle);
    } catch (AdbcException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    } finally {
      lifecycleLock.writeLock().unlock();
      arena.close();
      parent.release(this);
    }
    if (failure != null) {
      throw failure;
    }
  }
}
