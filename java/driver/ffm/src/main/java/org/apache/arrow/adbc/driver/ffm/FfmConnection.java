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
import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.BulkIngestMode;
import org.apache.arrow.adbc.core.IngestOption;
import org.apache.arrow.adbc.core.IsolationLevel;
import org.apache.arrow.adbc.core.TypedKey;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

final class FfmConnection implements AdbcConnection {
  private static final TypedKey<Boolean> AUTOCOMMIT =
      new TypedKey<>("adbc.connection.autocommit", Boolean.class);
  private static final TypedKey<String> ISOLATION_LEVEL =
      new TypedKey<>("adbc.connection.transaction.isolation_level", String.class);
  private static final TypedKey<Boolean> READONLY =
      new TypedKey<>("adbc.connection.readonly", Boolean.class);
  private static final TypedKey<String> CURRENT_CATALOG =
      new TypedKey<>("adbc.connection.catalog", String.class);
  private static final TypedKey<String> CURRENT_DB_SCHEMA =
      new TypedKey<>("adbc.connection.db_schema", String.class);

  private final BufferAllocator allocator;
  private final FfmDatabase parent;
  private final Arena arena;
  private final MemorySegment handle;
  private final Set<FfmStatement> statements = new LinkedHashSet<>();
  private final Set<ArrowReader> readers = new LinkedHashSet<>();
  private final Set<Arena> readerArenas = new LinkedHashSet<>();
  private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock(true);
  private boolean closed;

  FfmConnection(
      BufferAllocator allocator, FfmDatabase parent, Arena arena, MemorySegment handle) {
    this.allocator = allocator;
    this.parent = parent;
    this.arena = arena;
    this.handle = handle;
  }

  @Override
  public synchronized AdbcStatement createStatement() throws AdbcException {
    ensureOpen();
    Arena statementArena = NativeAdbc.newObjectArena();
    MemorySegment statement = NativeAdbc.newAdbcObject(statementArena);
    boolean allocated = false;
    try {
      NativeAdbc.statementNew(handle, statement);
      allocated = true;
      FfmStatement result = new FfmStatement(allocator, this, statementArena, statement);
      statements.add(result);
      return result;
    } catch (AdbcException | RuntimeException e) {
      if (allocated) {
        try {
          NativeAdbc.statementRelease(statement);
        } catch (Exception releaseError) {
          e.addSuppressed(releaseError);
        }
      }
      statementArena.close();
      if (e instanceof AdbcException) {
        throw (AdbcException) e;
      }
      throw AdbcException.internal("[ffm] failed to create statement").withCause(e);
    }
  }

  @Override
  public AdbcStatement bulkIngest(String targetTableName, BulkIngestMode mode)
      throws AdbcException {
    throw AdbcException.notImplemented("[ffm] bulk ingestion is not implemented");
  }

  @Override
  public AdbcStatement bulkIngest(
      String targetTableName, BulkIngestMode mode, IngestOption... options)
      throws AdbcException {
    throw AdbcException.notImplemented("[ffm] bulk ingestion is not implemented");
  }

  @Override
  public void cancel() throws AdbcException {
    lifecycleLock.readLock().lock();
    try {
      ensureOpen();
      NativeAdbc.connectionCancel(handle);
    } finally {
      lifecycleLock.readLock().unlock();
    }
  }

  @Override
  public synchronized void commit() throws AdbcException {
    ensureOpen();
    NativeAdbc.connectionCommit(handle);
  }

  @Override
  public synchronized void rollback() throws AdbcException {
    ensureOpen();
    NativeAdbc.connectionRollback(handle);
  }

  @Override
  public synchronized ArrowReader getInfo(int[] infoCodes) throws AdbcException {
    ensureOpen();
    return importStream(stream -> NativeAdbc.connectionGetInfo(handle, infoCodes, stream));
  }

  @Override
  public synchronized ArrowReader getObjects(
      GetObjectsDepth depth,
      String catalogPattern,
      String dbSchemaPattern,
      String tableNamePattern,
      String[] tableTypes,
      String columnNamePattern)
      throws AdbcException {
    ensureOpen();
    return importStream(
        stream ->
            NativeAdbc.connectionGetObjects(
                handle,
                depth.ordinal(),
                catalogPattern,
                dbSchemaPattern,
                tableNamePattern,
                tableTypes,
                columnNamePattern,
                stream));
  }

  @Override
  public synchronized Schema getTableSchema(String catalog, String dbSchema, String tableName)
      throws AdbcException {
    ensureOpen();
    try (ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
      NativeAdbc.connectionGetTableSchema(
          handle, catalog, dbSchema, tableName, schema.memoryAddress());
      return Data.importSchema(allocator, schema);
    }
  }

  @Override
  public synchronized ArrowReader getTableTypes() throws AdbcException {
    ensureOpen();
    return importStream(stream -> NativeAdbc.connectionGetTableTypes(handle, stream));
  }

  @Override
  public synchronized ArrowReader getStatistics(
      String catalogPattern, String dbSchemaPattern, String tableNamePattern, boolean approximate)
      throws AdbcException {
    ensureOpen();
    return importStream(
        stream ->
            NativeAdbc.connectionGetStatistics(
                handle,
                catalogPattern,
                dbSchemaPattern,
                tableNamePattern,
                approximate,
                stream));
  }

  @Override
  public synchronized ArrowReader getStatisticNames() throws AdbcException {
    ensureOpen();
    return importStream(stream -> NativeAdbc.connectionGetStatisticNames(handle, stream));
  }

  @Override
  public synchronized ArrowReader readPartition(ByteBuffer descriptor) throws AdbcException {
    ensureOpen();
    ByteBuffer view = descriptor.duplicate();
    byte[] bytes = new byte[view.remaining()];
    view.get(bytes);
    Arena descriptorArena = NativeAdbc.newObjectArena();
    try {
      MemorySegment nativeDescriptor = descriptorArena.allocate(bytes.length, 1);
      nativeDescriptor.copyFrom(MemorySegment.ofArray(bytes));
      ArrowReader reader = importStream(
          stream ->
              NativeAdbc.connectionReadPartition(
                  handle, nativeDescriptor, bytes.length, stream));
      try {
        ensureOpen();
        readerArenas.add(descriptorArena);
      } catch (AdbcException e) {
        try {
          reader.close();
        } catch (IOException closeError) {
          e.addSuppressed(closeError);
        }
        throw e;
      }
      return reader;
    } catch (AdbcException | RuntimeException e) {
      descriptorArena.close();
      throw e;
    }
  }

  private ArrowReader importStream(StreamCall call) throws AdbcException {
    try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
      call.invoke(stream.memoryAddress());
      ArrowReader reader = Data.importArrayStream(allocator, stream);
      synchronized (this) {
        try {
          ensureOpen();
        } catch (AdbcException e) {
          try {
            reader.close();
          } catch (IOException closeError) {
            e.addSuppressed(closeError);
          }
          throw e;
        }
        readers.add(reader);
      }
      return reader;
    }
  }

  @FunctionalInterface
  private interface StreamCall {
    void invoke(long streamAddress) throws AdbcException;
  }

  @Override
  public synchronized boolean getReadOnly() throws AdbcException {
    ensureOpen();
    return FfmOptions.get("Connection", handle, READONLY);
  }

  @Override
  public synchronized void setReadOnly(boolean readOnly) throws AdbcException {
    ensureOpen();
    FfmOptions.set("Connection", handle, READONLY, readOnly);
  }

  @Override
  public synchronized boolean getAutoCommit() throws AdbcException {
    ensureOpen();
    return FfmOptions.get("Connection", handle, AUTOCOMMIT);
  }

  @Override
  public synchronized void setAutoCommit(boolean autoCommit) throws AdbcException {
    ensureOpen();
    FfmOptions.set("Connection", handle, AUTOCOMMIT, autoCommit);
  }

  @Override
  public synchronized IsolationLevel getIsolationLevel() throws AdbcException {
    ensureOpen();
    String value = FfmOptions.get("Connection", handle, ISOLATION_LEVEL);
    if (value == null) {
      return null;
    }
    return switch (value) {
      case "adbc.connection.transaction.isolation.read_uncommitted" ->
          IsolationLevel.READ_UNCOMMITTED;
      case "adbc.connection.transaction.isolation.read_committed" ->
          IsolationLevel.READ_COMMITTED;
      case "adbc.connection.transaction.isolation.repeatable_read" ->
          IsolationLevel.REPEATABLE_READ;
      case "adbc.connection.transaction.isolation.snapshot" -> IsolationLevel.SNAPSHOT;
      case "adbc.connection.transaction.isolation.serializable" -> IsolationLevel.SERIALIZABLE;
      default -> throw AdbcException.invalidArgument("[ffm] invalid isolation level: " + value);
    };
  }

  @Override
  public synchronized void setIsolationLevel(IsolationLevel level) throws AdbcException {
    ensureOpen();
    String value =
        level == null
            ? null
            : switch (level) {
              case READ_UNCOMMITTED -> "adbc.connection.transaction.isolation.read_uncommitted";
              case READ_COMMITTED -> "adbc.connection.transaction.isolation.read_committed";
              case REPEATABLE_READ ->
                  "adbc.connection.transaction.isolation.repeatable_read";
              case SNAPSHOT -> "adbc.connection.transaction.isolation.snapshot";
              case SERIALIZABLE -> "adbc.connection.transaction.isolation.serializable";
            };
    FfmOptions.set("Connection", handle, ISOLATION_LEVEL, value);
  }

  @Override
  public synchronized String getCurrentCatalog() throws AdbcException {
    ensureOpen();
    return FfmOptions.get("Connection", handle, CURRENT_CATALOG);
  }

  @Override
  public synchronized String getCurrentDbSchema() throws AdbcException {
    ensureOpen();
    return FfmOptions.get("Connection", handle, CURRENT_DB_SCHEMA);
  }

  @Override
  public synchronized void setCurrentCatalog(String catalog) throws AdbcException {
    ensureOpen();
    FfmOptions.set("Connection", handle, CURRENT_CATALOG, catalog);
  }

  @Override
  public synchronized void setCurrentDbSchema(String dbSchema) throws AdbcException {
    ensureOpen();
    FfmOptions.set("Connection", handle, CURRENT_DB_SCHEMA, dbSchema);
  }

  @Override
  public synchronized <T> T getOption(TypedKey<T> key) throws AdbcException {
    ensureOpen();
    return FfmOptions.get("Connection", handle, key);
  }

  @Override
  public synchronized <T> void setOption(TypedKey<T> key, T value) throws AdbcException {
    ensureOpen();
    FfmOptions.set("Connection", handle, key, value);
  }

  synchronized void release(FfmStatement statement) {
    statements.remove(statement);
  }

  synchronized void ensureOpen() throws AdbcException {
    parent.ensureOpen();
    if (closed) {
      throw AdbcException.invalidState("[ffm] connection is closed");
    }
  }

  @Override
  public void close() throws AdbcException {
    ArrayList<FfmStatement> openStatements;
    ArrayList<ArrowReader> openReaders;
    ArrayList<Arena> openReaderArenas;
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      openStatements = new ArrayList<>(statements);
      openReaders = new ArrayList<>(readers);
      openReaderArenas = new ArrayList<>(readerArenas);
    }
    AdbcException failure = null;
    for (FfmStatement statement : openStatements) {
      try {
        statement.close();
      } catch (AdbcException e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    for (ArrowReader reader : openReaders) {
      try {
        reader.close();
      } catch (IOException e) {
        if (failure == null) {
          failure = AdbcException.internal("[ffm] failed to close result stream").withCause(e);
        } else {
          failure.addSuppressed(e);
        }
      }
    }
    synchronized (this) {
      readers.clear();
      readerArenas.clear();
    }
    lifecycleLock.writeLock().lock();
    try {
      NativeAdbc.connectionRelease(handle);
    } catch (AdbcException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    } finally {
      lifecycleLock.writeLock().unlock();
      for (Arena readerArena : openReaderArenas) {
        readerArena.close();
      }
      arena.close();
      parent.release(this);
    }
    if (failure != null) {
      throw failure;
    }
  }
}
