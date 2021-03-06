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
package org.flyte.flytekit;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.flyte.api.v1.Binding;
import org.flyte.api.v1.Node;
import org.flyte.api.v1.TypedInterface;
import org.flyte.api.v1.Variable;
import org.flyte.api.v1.WorkflowMetadata;
import org.flyte.api.v1.WorkflowTemplate;

class WorkflowTemplateIdl {

  public static WorkflowTemplate ofBuilder(SdkWorkflowBuilder builder) {
    WorkflowMetadata metadata = WorkflowMetadata.builder().build();

    List<Node> nodes =
        builder.getNodes().values().stream()
            .map(SdkNode::toIdl)
            .collect(collectingAndThen(toList(), Collections::unmodifiableList));

    List<Binding> outputBindings =
        builder.getOutputs().entrySet().stream()
            .map(entry -> getBinding(entry.getKey(), entry.getValue()))
            .collect(collectingAndThen(toList(), Collections::unmodifiableList));

    return WorkflowTemplate.builder()
        .metadata(metadata)
        .interface_(
            TypedInterface.builder()
                .inputs(getInputVariableMap(builder))
                .outputs(getOutputVariableMap(builder))
                .build())
        .outputs(outputBindings)
        .nodes(nodes)
        .build();
  }

  private static Map<String, Variable> getInputVariableMap(SdkWorkflowBuilder builder) {
    return builder.getInputs().entrySet().stream()
        .map(
            entry -> {
              Variable variable =
                  Variable.builder()
                      .literalType(entry.getValue().type())
                      .description(builder.getInputDescription(entry.getKey()))
                      .build();

              return new SimpleImmutableEntry<>(entry.getKey(), variable);
            })
        .collect(
            collectingAndThen(
                toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue),
                Collections::unmodifiableMap));
  }

  private static Map<String, Variable> getOutputVariableMap(SdkWorkflowBuilder builder) {
    return builder.getOutputs().entrySet().stream()
        .map(
            entry -> {
              Variable variable =
                  Variable.builder()
                      .literalType(entry.getValue().type())
                      .description(builder.getOutputDescription(entry.getKey()))
                      .build();

              return new SimpleImmutableEntry<>(entry.getKey(), variable);
            })
        .collect(
            collectingAndThen(
                toMap(SimpleImmutableEntry::getKey, SimpleImmutableEntry::getValue),
                Collections::unmodifiableMap));
  }

  private static Binding getBinding(String var_, SdkBindingData bindingData) {
    return Binding.builder().var_(var_).binding(bindingData.idl()).build();
  }
}
