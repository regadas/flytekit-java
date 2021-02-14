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

import static org.junit.jupiter.api.Assertions.assertEquals;

import flyteidl.flink.Flink.FlinkJob;
import java.util.Map;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;
import org.junit.jupiter.api.Test;

class ProtobufUtilTest {

  @Test
  void shouldConvertDefaultToLiteral() {
    FlinkJob instance = FlinkJob.getDefaultInstance();
    assertEquals(true, ProtobufUtil.literal(instance).isEmpty());

    FlinkJob withImage =
        instance
            .toBuilder()
            .setMainClass("org.flyte.Job")
            .setJarFile("gs://bucket/jar")
            .setServiceAccount("service-account")
            .addArgs("--output=gs://bucket/output")
            .setImage("flink-image")
            .build();
    Map<String, Literal> literals = ProtobufUtil.literal(withImage);

    assertEquals(false, literals.isEmpty());
    assertEquals(Literal.Kind.SCALAR, literals.get("mainClass").kind());
    assertEquals(Literal.Kind.COLLECTION, literals.get("args").kind());
  }

  @Test
  void shouldConvertDefaultToVariableMap() {
    Map<String, Variable> variableMap = ProtobufUtil.variableMap(FlinkJob.getDefaultInstance());
    assertEquals(false, variableMap.isEmpty());

    assertEquals(
        LiteralType.Kind.SIMPLE_TYPE, variableMap.get("mainClass").literalType().getKind());
    assertEquals(LiteralType.Kind.COLLECTION_TYPE, variableMap.get("args").literalType().getKind());
  }
}
