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

import java.lang.foreign.MemorySegment;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.TypedKey;

final class FfmOptions {
  private FfmOptions() {}

  static <T> T get(String kind, MemorySegment object, TypedKey<T> key) throws AdbcException {
    Class<T> type = key.getType();
    Object value;
    if (type == String.class) {
      value = NativeAdbc.getOption(kind, object, key.getKey());
    } else if (type == Long.class) {
      value = NativeAdbc.getOptionInt(kind, object, key.getKey());
    } else if (type == Integer.class) {
      value = Math.toIntExact(NativeAdbc.getOptionInt(kind, object, key.getKey()));
    } else if (type == Double.class) {
      value = NativeAdbc.getOptionDouble(kind, object, key.getKey());
    } else if (type == Float.class) {
      value = (float) NativeAdbc.getOptionDouble(kind, object, key.getKey());
    } else if (type == Boolean.class) {
      String text = NativeAdbc.getOption(kind, object, key.getKey());
      if ("true".equalsIgnoreCase(text)) {
        value = true;
      } else if ("false".equalsIgnoreCase(text)) {
        value = false;
      } else {
        throw AdbcException.invalidArgument(
            "[ffm] invalid boolean option value for " + key.getKey() + ": " + text);
      }
    } else if (type == byte[].class) {
      value = NativeAdbc.getOptionBytes(kind, object, key.getKey());
    } else {
      throw AdbcException.notImplemented("[ffm] unsupported option type " + type.getName());
    }
    return key.cast(value);
  }

  static <T> void set(
      String kind, MemorySegment object, TypedKey<T> key, T value) throws AdbcException {
    if (value == null || value instanceof String) {
      NativeAdbc.setOption(kind, object, key.getKey(), (String) value);
    } else if (value instanceof Integer) {
      NativeAdbc.setOptionInt(kind, object, key.getKey(), (Integer) value);
    } else if (value instanceof Long) {
      NativeAdbc.setOptionInt(kind, object, key.getKey(), (Long) value);
    } else if (value instanceof Float) {
      NativeAdbc.setOptionDouble(kind, object, key.getKey(), (Float) value);
    } else if (value instanceof Double) {
      NativeAdbc.setOptionDouble(kind, object, key.getKey(), (Double) value);
    } else if (value instanceof Boolean) {
      NativeAdbc.setOption(kind, object, key.getKey(), value.toString());
    } else if (value instanceof byte[]) {
      NativeAdbc.setOptionBytes(kind, object, key.getKey(), (byte[]) value);
    } else {
      throw AdbcException.notImplemented("[ffm] unsupported option type " + value.getClass());
    }
  }
}
