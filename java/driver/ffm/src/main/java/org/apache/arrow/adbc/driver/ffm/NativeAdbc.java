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
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatusCode;

/** Low-level ADBC driver-manager calls. Struct layouts are derived from the ADBC C headers. */
final class NativeAdbc {
  private static final Linker LINKER = Linker.nativeLinker();
  private static final ValueLayout.OfByte C_STATUS = ValueLayout.JAVA_BYTE;
  private static final ValueLayout.OfInt C_INT = ValueLayout.JAVA_INT;
  private static final ValueLayout.OfLong C_INT64 = ValueLayout.JAVA_LONG;
  private static final ValueLayout.OfLong C_SIZE_T;
  private static final MemoryLayout ERROR_LAYOUT =
      MemoryLayout.structLayout(
          ValueLayout.ADDRESS.withName("message"),
          C_INT.withName("vendor_code"),
          MemoryLayout.sequenceLayout(5, ValueLayout.JAVA_BYTE).withName("sqlstate"),
          MemoryLayout.paddingLayout(7),
          ValueLayout.ADDRESS.withName("release"),
          ValueLayout.ADDRESS.withName("private_data"),
          ValueLayout.ADDRESS.withName("private_driver"));
  private static final long ERROR_MESSAGE_OFFSET = ERROR_LAYOUT.byteOffset(
      MemoryLayout.PathElement.groupElement("message"));
  private static final long ERROR_VENDOR_OFFSET = ERROR_LAYOUT.byteOffset(
      MemoryLayout.PathElement.groupElement("vendor_code"));
  private static final long ERROR_SQLSTATE_OFFSET = ERROR_LAYOUT.byteOffset(
      MemoryLayout.PathElement.groupElement("sqlstate"));
  private static final long ERROR_RELEASE_OFFSET = ERROR_LAYOUT.byteOffset(
      MemoryLayout.PathElement.groupElement("release"));
  private static final SymbolLookup LOOKUP;

  static {
    MemoryLayout sizeT = LINKER.canonicalLayouts().get("size_t");
    if (!(sizeT instanceof ValueLayout.OfLong)) {
      throw new ExceptionInInitializerError(
          "The FFM ADBC driver currently requires a 64-bit size_t ABI");
    }
    C_SIZE_T = (ValueLayout.OfLong) sizeT;
    if (ValueLayout.ADDRESS.byteSize() != 8 || ERROR_LAYOUT.byteSize() != 48) {
      throw new ExceptionInInitializerError(
          "Unsupported C ABI: expected 64-bit pointers and the ADBC 1.1 AdbcError layout");
    }

    String managerPath = System.getProperty("arrow.adbc.driver.ffm.manager.path");
    try {
      if (managerPath == null || managerPath.isBlank()) {
        System.loadLibrary("adbc_driver_manager");
      } else {
        System.load(managerPath);
      }
    } catch (UnsatisfiedLinkError e) {
      throw new ExceptionInInitializerError(
          "Could not load the ADBC driver manager. Set "
              + "arrow.adbc.driver.ffm.manager.path to its absolute path.");
    }
    LOOKUP = SymbolLookup.loaderLookup();
  }

  private NativeAdbc() {}

  static Arena newObjectArena() {
    return Arena.ofShared();
  }

  static MemorySegment newAdbcObject(Arena arena) {
    return arena.allocate(2 * ValueLayout.ADDRESS.byteSize(), ValueLayout.ADDRESS.byteAlignment())
        .fill((byte) 0);
  }

  static MemorySegment cString(Arena arena, String value) {
    if (value == null) {
      return MemorySegment.NULL;
    }
    MemorySegment result =
        arena.allocate(value.getBytes(StandardCharsets.UTF_8).length + 1L, 1);
    result.setString(0, value, StandardCharsets.UTF_8);
    return result;
  }

  static MemorySegment pointer(long address) {
    return address == 0 ? MemorySegment.NULL : MemorySegment.ofAddress(address);
  }

  static void databaseNew(MemorySegment database) throws AdbcException {
    status("AdbcDatabaseNew", arg(ValueLayout.ADDRESS, database));
  }

  static void setLoadFlags(MemorySegment database) throws AdbcException {
    status(
        "AdbcDriverManagerDatabaseSetLoadFlags",
        arg(ValueLayout.ADDRESS, database),
        arg(C_INT, 15));
  }

  static void setAdditionalSearchPath(MemorySegment database, String path) throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "AdbcDriverManagerDatabaseSetAdditionalSearchPathList",
          arg(ValueLayout.ADDRESS, database),
          arg(ValueLayout.ADDRESS, cString(arena, path)));
    }
  }

  static void databaseSetOption(MemorySegment database, String key, String value)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "AdbcDatabaseSetOption",
          arg(ValueLayout.ADDRESS, database),
          arg(ValueLayout.ADDRESS, cString(arena, key)),
          arg(ValueLayout.ADDRESS, cString(arena, value)));
    }
  }

  static void databaseInit(MemorySegment database) throws AdbcException {
    status("AdbcDatabaseInit", arg(ValueLayout.ADDRESS, database));
  }

  static void databaseRelease(MemorySegment database) throws AdbcException {
    status("AdbcDatabaseRelease", arg(ValueLayout.ADDRESS, database));
  }

  static void connectionNew(MemorySegment connection) throws AdbcException {
    status("AdbcConnectionNew", arg(ValueLayout.ADDRESS, connection));
  }

  static void connectionInit(MemorySegment connection, MemorySegment database)
      throws AdbcException {
    status(
        "AdbcConnectionInit",
        arg(ValueLayout.ADDRESS, connection),
        arg(ValueLayout.ADDRESS, database));
  }

  static void connectionRelease(MemorySegment connection) throws AdbcException {
    status("AdbcConnectionRelease", arg(ValueLayout.ADDRESS, connection));
  }

  static void connectionCancel(MemorySegment connection) throws AdbcException {
    status("AdbcConnectionCancel", arg(ValueLayout.ADDRESS, connection));
  }

  static void connectionCommit(MemorySegment connection) throws AdbcException {
    status("AdbcConnectionCommit", arg(ValueLayout.ADDRESS, connection));
  }

  static void connectionRollback(MemorySegment connection) throws AdbcException {
    status("AdbcConnectionRollback", arg(ValueLayout.ADDRESS, connection));
  }

  static void statementNew(MemorySegment connection, MemorySegment statement)
      throws AdbcException {
    status(
        "AdbcStatementNew",
        arg(ValueLayout.ADDRESS, connection),
        arg(ValueLayout.ADDRESS, statement));
  }

  static void statementRelease(MemorySegment statement) throws AdbcException {
    status("AdbcStatementRelease", arg(ValueLayout.ADDRESS, statement));
  }

  static void statementCancel(MemorySegment statement) throws AdbcException {
    status("AdbcStatementCancel", arg(ValueLayout.ADDRESS, statement));
  }

  static void statementPrepare(MemorySegment statement) throws AdbcException {
    status("AdbcStatementPrepare", arg(ValueLayout.ADDRESS, statement));
  }

  static void statementSetSqlQuery(MemorySegment statement, String query) throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "AdbcStatementSetSqlQuery",
          arg(ValueLayout.ADDRESS, statement),
          arg(ValueLayout.ADDRESS, cString(arena, query)));
    }
  }

  static void statementSetSubstraitPlan(MemorySegment statement, MemorySegment plan, long length)
      throws AdbcException {
    status(
        "AdbcStatementSetSubstraitPlan",
        arg(ValueLayout.ADDRESS, statement),
        arg(ValueLayout.ADDRESS, plan),
        arg(C_SIZE_T, length));
  }

  static long statementExecuteQuery(MemorySegment statement, long streamAddress)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment rowsAffected = arena.allocate(C_INT64);
      rowsAffected.set(C_INT64, 0, -1L);
      status(
          "AdbcStatementExecuteQuery",
          arg(ValueLayout.ADDRESS, statement),
          arg(ValueLayout.ADDRESS, pointer(streamAddress)),
          arg(ValueLayout.ADDRESS, rowsAffected));
      return rowsAffected.get(C_INT64, 0);
    }
  }

  static void statementExecuteSchema(MemorySegment statement, long schemaAddress)
      throws AdbcException {
    status(
        "AdbcStatementExecuteSchema",
        arg(ValueLayout.ADDRESS, statement),
        arg(ValueLayout.ADDRESS, pointer(schemaAddress)));
  }

  static void statementBind(
      MemorySegment statement, long arrayAddress, long schemaAddress) throws AdbcException {
    status(
        "AdbcStatementBind",
        arg(ValueLayout.ADDRESS, statement),
        arg(ValueLayout.ADDRESS, pointer(arrayAddress)),
        arg(ValueLayout.ADDRESS, pointer(schemaAddress)));
  }

  static void statementBindStream(MemorySegment statement, long streamAddress)
      throws AdbcException {
    status(
        "AdbcStatementBindStream",
        arg(ValueLayout.ADDRESS, statement),
        arg(ValueLayout.ADDRESS, pointer(streamAddress)));
  }

  static void statementSetOption(MemorySegment statement, String key, String value)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "AdbcStatementSetOption",
          arg(ValueLayout.ADDRESS, statement),
          arg(ValueLayout.ADDRESS, cString(arena, key)),
          arg(ValueLayout.ADDRESS, cString(arena, value)));
    }
  }

  static void connectionSetOption(MemorySegment connection, String key, String value)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "AdbcConnectionSetOption",
          arg(ValueLayout.ADDRESS, connection),
          arg(ValueLayout.ADDRESS, cString(arena, key)),
          arg(ValueLayout.ADDRESS, cString(arena, value)));
    }
  }

  static void setOption(String objectKind, MemorySegment object, String key, String value)
      throws AdbcException {
    switch (objectKind) {
      case "Database" -> databaseSetOption(object, key, value);
      case "Connection" -> connectionSetOption(object, key, value);
      case "Statement" -> statementSetOption(object, key, value);
      default -> throw AdbcException.invalidArgument("Unknown ADBC object kind: " + objectKind);
    }
  }

  static String getOption(String objectKind, MemorySegment object, String key)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment buffer = arena.allocate(64 * 1024L, 1);
      MemorySegment length = arena.allocate(C_SIZE_T);
      length.set(C_SIZE_T, 0, buffer.byteSize());
      MemorySegment cKey = cString(arena, key);
      status(
          "Adbc" + objectKind + "GetOption",
          arg(ValueLayout.ADDRESS, object),
          arg(ValueLayout.ADDRESS, cKey),
          arg(ValueLayout.ADDRESS, buffer),
          arg(ValueLayout.ADDRESS, length));
      long resultLength = length.get(C_SIZE_T, 0);
      if (resultLength > buffer.byteSize()) {
        throw AdbcException.invalidArgument(
            "ADBC option value exceeds the FFM driver's 64 KiB string limit: " + key);
      }
      byte[] bytes = buffer.asSlice(0, resultLength).toArray(ValueLayout.JAVA_BYTE);
      int nul = 0;
      while (nul < bytes.length && bytes[nul] != 0) {
        nul++;
      }
      return new String(bytes, 0, nul, StandardCharsets.UTF_8);
    }
  }

  static long getOptionInt(String objectKind, MemorySegment object, String key)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment value = arena.allocate(C_INT64);
      status(
          "Adbc" + objectKind + "GetOptionInt",
          arg(ValueLayout.ADDRESS, object),
          arg(ValueLayout.ADDRESS, cString(arena, key)),
          arg(ValueLayout.ADDRESS, value));
      return value.get(C_INT64, 0);
    }
  }

  static double getOptionDouble(String objectKind, MemorySegment object, String key)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment value = arena.allocate(ValueLayout.JAVA_DOUBLE);
      status(
          "Adbc" + objectKind + "GetOptionDouble",
          arg(ValueLayout.ADDRESS, object),
          arg(ValueLayout.ADDRESS, cString(arena, key)),
          arg(ValueLayout.ADDRESS, value));
      return value.get(ValueLayout.JAVA_DOUBLE, 0);
    }
  }

  static byte[] getOptionBytes(String objectKind, MemorySegment object, String key)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment length = arena.allocate(C_SIZE_T);
      length.set(C_SIZE_T, 0, 0);
      MemorySegment cKey = cString(arena, key);
      MemorySegment probe = arena.allocate(1, 1);
      status(
          "Adbc" + objectKind + "GetOptionBytes",
          arg(ValueLayout.ADDRESS, object),
          arg(ValueLayout.ADDRESS, cKey),
          arg(ValueLayout.ADDRESS, probe),
          arg(ValueLayout.ADDRESS, length));
      long required = length.get(C_SIZE_T, 0);
      if (required < 0 || required > Integer.MAX_VALUE) {
        throw AdbcException.invalidArgument(
            "Invalid byte option size for " + key + ": " + required);
      }
      MemorySegment bytes = arena.allocate(Math.max(required, 1), 1);
      length.set(C_SIZE_T, 0, required);
      status(
          "Adbc" + objectKind + "GetOptionBytes",
          arg(ValueLayout.ADDRESS, object),
          arg(ValueLayout.ADDRESS, cKey),
          arg(ValueLayout.ADDRESS, bytes),
          arg(ValueLayout.ADDRESS, length));
      long actual = length.get(C_SIZE_T, 0);
      if (actual > required) {
        throw AdbcException.invalidArgument("Byte option changed size while being read: " + key);
      }
      return bytes.asSlice(0, actual).toArray(ValueLayout.JAVA_BYTE);
    }
  }

  static void setOptionInt(String objectKind, MemorySegment object, String key, long value)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "Adbc" + objectKind + "SetOptionInt",
          arg(ValueLayout.ADDRESS, object),
          arg(ValueLayout.ADDRESS, cString(arena, key)),
          arg(C_INT64, value));
    }
  }

  static void setOptionDouble(String objectKind, MemorySegment object, String key, double value)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "Adbc" + objectKind + "SetOptionDouble",
          arg(ValueLayout.ADDRESS, object),
          arg(ValueLayout.ADDRESS, cString(arena, key)),
          arg(ValueLayout.JAVA_DOUBLE, value));
    }
  }

  static void setOptionBytes(
      String objectKind, MemorySegment object, String key, byte[] value) throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment bytes = arena.allocate(value.length, 1);
      if (value.length > 0) {
        bytes.copyFrom(MemorySegment.ofArray(value));
      }
      status(
          "Adbc" + objectKind + "SetOptionBytes",
          arg(ValueLayout.ADDRESS, object),
          arg(ValueLayout.ADDRESS, cString(arena, key)),
          arg(ValueLayout.ADDRESS, bytes),
          arg(C_SIZE_T, value.length));
    }
  }

  static void connectionGetInfo(MemorySegment connection, int[] infoCodes, long streamAddress)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment codes = MemorySegment.NULL;
      long count = 0;
      if (infoCodes != null && infoCodes.length > 0) {
        codes = arena.allocate((long) infoCodes.length * C_INT.byteSize(), C_INT.byteAlignment());
        for (int i = 0; i < infoCodes.length; i++) {
          codes.setAtIndex(C_INT, i, infoCodes[i]);
        }
        count = infoCodes.length;
      }
      status(
          "AdbcConnectionGetInfo",
          arg(ValueLayout.ADDRESS, connection),
          arg(ValueLayout.ADDRESS, codes),
          arg(C_SIZE_T, count),
          arg(ValueLayout.ADDRESS, pointer(streamAddress)));
    }
  }

  static void connectionGetTableTypes(MemorySegment connection, long streamAddress)
      throws AdbcException {
    status(
        "AdbcConnectionGetTableTypes",
        arg(ValueLayout.ADDRESS, connection),
        arg(ValueLayout.ADDRESS, pointer(streamAddress)));
  }

  static void connectionGetObjects(
      MemorySegment connection,
      int depth,
      String catalog,
      String dbSchema,
      String table,
      String[] tableTypes,
      String column,
      long streamAddress)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment types = MemorySegment.NULL;
      if (tableTypes != null) {
        types = arena.allocate((long) (tableTypes.length + 1) * ValueLayout.ADDRESS.byteSize(),
            ValueLayout.ADDRESS.byteAlignment());
        for (int i = 0; i < tableTypes.length; i++) {
          types.setAtIndex(ValueLayout.ADDRESS, i, cString(arena, tableTypes[i]));
        }
        types.setAtIndex(ValueLayout.ADDRESS, tableTypes.length, MemorySegment.NULL);
      }
      status(
          "AdbcConnectionGetObjects",
          arg(ValueLayout.ADDRESS, connection),
          arg(C_INT, depth),
          arg(ValueLayout.ADDRESS, cString(arena, catalog)),
          arg(ValueLayout.ADDRESS, cString(arena, dbSchema)),
          arg(ValueLayout.ADDRESS, cString(arena, table)),
          arg(ValueLayout.ADDRESS, types),
          arg(ValueLayout.ADDRESS, cString(arena, column)),
          arg(ValueLayout.ADDRESS, pointer(streamAddress)));
    }
  }

  static void connectionGetTableSchema(
      MemorySegment connection, String catalog, String schema, String table, long outputAddress)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "AdbcConnectionGetTableSchema",
          arg(ValueLayout.ADDRESS, connection),
          arg(ValueLayout.ADDRESS, cString(arena, catalog)),
          arg(ValueLayout.ADDRESS, cString(arena, schema)),
          arg(ValueLayout.ADDRESS, cString(arena, table)),
          arg(ValueLayout.ADDRESS, pointer(outputAddress)));
    }
  }

  static void connectionGetStatistics(
      MemorySegment connection,
      String catalog,
      String schema,
      String table,
      boolean approximate,
      long streamAddress)
      throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      status(
          "AdbcConnectionGetStatistics",
          arg(ValueLayout.ADDRESS, connection),
          arg(ValueLayout.ADDRESS, cString(arena, catalog)),
          arg(ValueLayout.ADDRESS, cString(arena, schema)),
          arg(ValueLayout.ADDRESS, cString(arena, table)),
          arg(ValueLayout.JAVA_BYTE, (byte) (approximate ? 1 : 0)),
          arg(ValueLayout.ADDRESS, pointer(streamAddress)));
    }
  }

  static void connectionGetStatisticNames(MemorySegment connection, long streamAddress)
      throws AdbcException {
    status(
        "AdbcConnectionGetStatisticNames",
        arg(ValueLayout.ADDRESS, connection),
        arg(ValueLayout.ADDRESS, pointer(streamAddress)));
  }

  static void connectionReadPartition(
      MemorySegment connection, MemorySegment descriptor, long length, long streamAddress)
      throws AdbcException {
    status(
        "AdbcConnectionReadPartition",
        arg(ValueLayout.ADDRESS, connection),
        arg(ValueLayout.ADDRESS, descriptor),
        arg(C_SIZE_T, length),
        arg(ValueLayout.ADDRESS, pointer(streamAddress)));
  }

  static void statementGetParameterSchema(MemorySegment statement, long outputAddress)
      throws AdbcException {
    status(
        "AdbcStatementGetParameterSchema",
        arg(ValueLayout.ADDRESS, statement),
        arg(ValueLayout.ADDRESS, pointer(outputAddress)));
  }

  private static Arg arg(MemoryLayout layout, Object value) {
    return new Arg(layout, value);
  }

  private record Arg(MemoryLayout layout, Object value) {}

  private static void status(String name, Arg... arguments) throws AdbcException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment error = arena.allocate(ERROR_LAYOUT).fill((byte) 0);
      List<MemoryLayout> layouts = new ArrayList<>(arguments.length + 1);
      List<Object> values = new ArrayList<>(arguments.length + 1);
      for (Arg argument : arguments) {
        layouts.add(argument.layout());
        values.add(argument.value());
      }
      layouts.add(ValueLayout.ADDRESS);
      values.add(error);
      FunctionDescriptor descriptor =
          FunctionDescriptor.of(C_STATUS, layouts.toArray(MemoryLayout[]::new));
      byte code = ((Number) invoke(name, descriptor, values)).byteValue();
      if (code == 0) {
        return;
      }
      MemorySegment messagePointer = error.get(ValueLayout.ADDRESS, ERROR_MESSAGE_OFFSET);
      String message =
          messagePointer.equals(MemorySegment.NULL)
              ? name + " failed"
              : messagePointer.reinterpret(64 * 1024L).getString(0);
      String sqlState = readSqlState(error);
      int vendorCode = error.get(C_INT, ERROR_VENDOR_OFFSET);
      AdbcStatusCode javaCode =
          code > 0 && code <= AdbcStatusCode.values().length
              ? AdbcStatusCode.values()[code - 1]
              : AdbcStatusCode.UNKNOWN;
      releaseError(error);
      throw new AdbcException(message, null, javaCode, sqlState, vendorCode);
    } catch (AdbcException e) {
      throw e;
    } catch (Throwable e) {
      throw AdbcException.internal("Failed to call ADBC function " + name).withCause(e);
    }
  }

  private static String readSqlState(MemorySegment error) {
    byte[] bytes =
        error.asSlice(ERROR_SQLSTATE_OFFSET, 5).toArray(ValueLayout.JAVA_BYTE);
    int length = 0;
    while (length < bytes.length && bytes[length] != 0) {
      length++;
    }
    return length == 0 ? null : new String(bytes, 0, length, StandardCharsets.US_ASCII);
  }

  private static void releaseError(MemorySegment error) throws Throwable {
    MemorySegment release = error.get(ValueLayout.ADDRESS, ERROR_RELEASE_OFFSET);
    if (!release.equals(MemorySegment.NULL)) {
      MethodHandle handle =
          LINKER.downcallHandle(release, FunctionDescriptor.of(C_STATUS, ValueLayout.ADDRESS));
      handle.invokeWithArguments(error);
    }
  }

  private static Object invoke(
      String name, FunctionDescriptor descriptor, List<Object> arguments) throws Throwable {
    MemorySegment address =
        LOOKUP.find(name)
            .orElseThrow(() -> new UnsatisfiedLinkError("Missing ADBC symbol: " + name));
    MethodHandle handle = LINKER.downcallHandle(address, descriptor);
    return handle.invokeWithArguments(arguments);
  }

  private static Object invoke(
      String name, FunctionDescriptor descriptor, Object... arguments) throws Throwable {
    return invoke(name, descriptor, List.of(arguments));
  }
}
