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

import static java.util.Collections.singletonMap;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import com.google.auto.value.AutoValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Variable;

@SuppressWarnings("PreferJavaTimeOverload")
@AutoValue
public abstract class SdkLaunchPlan {

  public abstract String name();

  @Nullable
  public abstract String workflowProject();

  @Nullable
  public abstract String workflowDomain();

  public abstract String workflowName();

  @Nullable
  public abstract String workflowVersion();

  abstract Map<String, LiteralType> workflowInputTypeMap();

  abstract Map<String, Literal> fixedInputs();

  @Nullable
  public abstract SdkCronSchedule cronSchedule();

  /**
   * Creates a launch plan for specified {@link SdkLaunchPlan} with default naming, no inputs and no
   * schedule. The default launch plan name is {@link SdkWorkflow#getName()}. New name, inputs and
   * schedule could be added by calling {@link #withName(String)}, {@link #withFixedInput(String,
   * long)} (and friends respectively) and {@link #withCronSchedule(SdkCronSchedule)}.
   *
   * @param workflow Workflow to be reference by new {@link SdkLaunchPlan}.
   * @return the created {@link SdkLaunchPlan}.
   */
  public static SdkLaunchPlan of(SdkWorkflow workflow) {
    SdkWorkflowBuilder wfBuilder = new SdkWorkflowBuilder();
    workflow.expand(wfBuilder);
    return builder()
        .name(workflow.getName())
        .workflowName(workflow.getName())
        .workflowInputTypeMap(toWorkflowInputTypeMap(wfBuilder.getInputs(), SdkBindingData::type))
        .build();
  }

  private static <T> Map<String, LiteralType> toWorkflowInputTypeMap(
      Map<String, T> inputMap, Function<T, LiteralType> toLiteralTypeFn) {
    return inputMap.keySet().stream()
        .collect(
            collectingAndThen(
                toMap(identity(), name -> toLiteralTypeFn.apply(inputMap.get(name))),
                Collections::unmodifiableMap));
  }

  public SdkLaunchPlan withName(String newName) {
    return toBuilder().name(newName).build();
  }

  public SdkLaunchPlan withFixedInput(String inputName, long value) {
    return withFixedInputs0(
        singletonMap(inputName, Literals.ofInteger(value)),
        singletonMap(inputName, LiteralTypes.INTEGER));
  }

  public SdkLaunchPlan withFixedInput(String inputName, double value) {
    return withFixedInputs0(
        singletonMap(inputName, Literals.ofFloat(value)),
        singletonMap(inputName, LiteralTypes.FLOAT));
  }

  public SdkLaunchPlan withFixedInput(String inputName, String value) {
    return withFixedInputs0(
        singletonMap(inputName, Literals.ofString(value)),
        singletonMap(inputName, LiteralTypes.STRING));
  }

  public SdkLaunchPlan withFixedInput(String inputName, boolean value) {
    return withFixedInputs0(
        singletonMap(inputName, Literals.ofBoolean(value)),
        singletonMap(inputName, LiteralTypes.BOOLEAN));
  }

  public SdkLaunchPlan withFixedInput(String inputName, Instant value) {
    return withFixedInputs0(
        singletonMap(inputName, Literals.ofDatetime(value)),
        singletonMap(inputName, LiteralTypes.DATETIME));
  }

  public SdkLaunchPlan withFixedInput(String inputName, Duration value) {
    return withFixedInputs0(
        singletonMap(inputName, Literals.ofDuration(value)),
        singletonMap(inputName, LiteralTypes.DURATION));
  }

  public <T> SdkLaunchPlan withFixedInputs(SdkType<T> type, T value) {
    return withFixedInputs0(
        type.toLiteralMap(value),
        toWorkflowInputTypeMap(type.getVariableMap(), Variable::literalType));
  }

  public SdkLaunchPlan withCronSchedule(SdkCronSchedule cronSchedule) {
    return toBuilder().cronSchedule(cronSchedule).build();
  }

  private SdkLaunchPlan withFixedInputs0(
      Map<String, Literal> newInputs, Map<String, LiteralType> newInputTypes) {

    verifyNonEmptyWorkflowInput(newInputTypes);
    verifyMatchedInput(newInputTypes);

    Map<String, Literal> newCompleteInputs = new LinkedHashMap<>(fixedInputs());
    newInputs.forEach(
        (inputName, value) -> {
          Literal previous = newCompleteInputs.put(inputName, value);
          if (previous != null) {
            throw new IllegalArgumentException(
                String.format(
                    "Duplicate input [%s] on launch plan [%s], values: [%s] [%s]",
                    inputName, name(), value, previous));
          }
        });

    return toBuilder().fixedInputs(newCompleteInputs).build();
  }

  private void verifyNonEmptyWorkflowInput(Map<String, LiteralType> newInputTypes) {
    if (workflowInputTypeMap().isEmpty() && !newInputTypes.isEmpty()) {
      String message =
          String.format(
              "invalid launch plan fixed inputs, expected none but found %s", newInputTypes.size());
      throw new IllegalArgumentException(message);
    }
  }

  private void verifyMatchedInput(Map<String, LiteralType> newInputTypes) {
    for (Map.Entry<String, LiteralType> lpInputType : newInputTypes.entrySet()) {
      String inputName = lpInputType.getKey();
      LiteralType lpType = lpInputType.getValue();

      LiteralType wfType = workflowInputTypeMap().get(inputName);

      if (wfType == null) {
        String message = String.format("unexpected fixed input %s", inputName);
        throw new IllegalArgumentException(message);
      }
      if (!lpType.equals(wfType)) {
        String message =
            String.format(
                "invalid fixed input wrong type %s, expected %s, got %s instead",
                inputName, wfType, lpType);
        throw new IllegalArgumentException(message);
      }
    }
  }

  static Builder builder() {
    return new AutoValue_SdkLaunchPlan.Builder()
        .fixedInputs(Collections.emptyMap())
        .workflowInputTypeMap(Collections.emptyMap());
  }

  abstract Builder toBuilder();

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder name(String name);

    abstract Builder workflowProject(String workflowProject);

    abstract Builder workflowDomain(String workflowDomain);

    abstract Builder workflowName(String workflowName);

    abstract Builder workflowVersion(String workflowVersion);

    abstract Builder fixedInputs(Map<String, Literal> fixedInputs);

    abstract Builder cronSchedule(SdkCronSchedule cronSchedule);

    abstract Builder workflowInputTypeMap(Map<String, LiteralType> workflowInputTypeMap);

    abstract SdkLaunchPlan build();
  }
}
