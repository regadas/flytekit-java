/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.flyte.flytekit.flink;

import com.google.protobuf.Message;
import java.util.Map;
import java.util.Objects;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkType;

public class MessageSdkType<T extends Message> extends SdkType<T> {

  private final Class<T> clazz;
  private final Map<String, Variable> variableMap;

  private MessageSdkType(Class<T> clazz, Map<String, Variable> variableMap) {
    this.clazz = Objects.requireNonNull(clazz);
    this.variableMap = Objects.requireNonNull(variableMap);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Message> MessageSdkType<T> of(Class<T> clazz) {
    try {
      T instance = (T) clazz.getMethod("getDefaultInstance").invoke(null);
      return new MessageSdkType<>(clazz, ProtobufUtil.variableMap(instance));
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not get default instance");
    }
  }

  @Override
  public Map<String, Literal> toLiteralMap(T value) {
    return ProtobufUtil.literal(value);
  }

  @Override
  public T fromLiteralMap(Map<String, Literal> value) {
    try {
      return ProtobufUtil.fromLiteralMap(value, clazz);
    } catch (Exception e) {
      throw new IllegalArgumentException();
    }
  }

  @Override
  public Map<String, Variable> getVariableMap() {
    return variableMap;
  }
}
