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
package org.flyte.api.v1;

import com.google.auto.value.AutoOneOf;
import java.time.Duration;
import java.time.Instant;

/** A simple value. Primitive can be: string, ... . */
@AutoOneOf(SimpleType.class)
public abstract class Primitive {
  public abstract SimpleType type();

  public abstract long integer();

  public abstract double float_();

  public abstract String string();

  public abstract boolean boolean_();

  public abstract Instant datetime();

  public abstract Duration duration();

  public static Primitive ofInteger(long integer) {
    return AutoOneOf_Primitive.integer(integer);
  }

  public static Primitive ofFloat(double float_) {
    return AutoOneOf_Primitive.float_(float_);
  }

  public static Primitive ofString(String string) {
    return AutoOneOf_Primitive.string(string);
  }

  public static Primitive ofBoolean(boolean boolean_) {
    return AutoOneOf_Primitive.boolean_(boolean_);
  }

  public static Primitive ofDatetime(Instant datetime) {
    return AutoOneOf_Primitive.datetime(datetime);
  }

  public static Primitive ofDuration(Duration duration) {
    return AutoOneOf_Primitive.duration(duration);
  }

  public Object value() {
    switch (type()) {
      case INTEGER:
        return integer();
      case FLOAT:
        return float_();
      case STRING:
        return string();
      case BOOLEAN:
        return boolean_();
      case DATETIME:
        return datetime();
      case DURATION:
        return duration();
      default:
        throw new IllegalArgumentException();
    }
  }
}
