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

import static java.util.function.Function.identity;

import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.flyte.api.v1.Literal;
import org.flyte.api.v1.LiteralType;
import org.flyte.api.v1.Primitive;
import org.flyte.api.v1.Scalar;
import org.flyte.api.v1.SchemaType;
import org.flyte.api.v1.SchemaType.Column;
import org.flyte.api.v1.SchemaType.ColumnType;
import org.flyte.api.v1.SimpleType;
import org.flyte.api.v1.Variable;
import org.flyte.flytekit.SdkBindingData;

public final class ProtobufUtil {
  private static final String TYPE_NOT_SUPPORTED_FMT = "type %s not supported";

  private ProtobufUtil() {}

  public static Map<String, Variable> variableMap(Message message) {
    return message.getDescriptorForType().getFields().stream()
        .map(
            fd -> {
              Object value = message.getField(fd);
              Variable.Builder builder = Variable.builder().description(fd.getName());
              if (fd.isRepeated()) {
                builder.literalType(LiteralType.ofCollectionType(literalType(fd, value))).build();
              } else {
                builder.literalType(literalType(fd, value));
              }
              return builder.build();
            })
        .collect(Collectors.toMap(Variable::description, identity()));
  }

  public static SchemaType schemaType(Message message) {
    Map<FieldDescriptor, Object> allFields = message.getAllFields();
    List<Column> columns =
        allFields.entrySet().stream()
            .map(
                entry -> {
                  FieldDescriptor fd = entry.getKey();
                  return Column.builder()
                      .name(fd.getName())
                      .type(columnType(fd, entry.getValue()))
                      .build();
                })
            .collect(Collectors.toList());

    return SchemaType.builder().columns(columns).build();
  }

  static ColumnType columnType(FieldDescriptor fd, Object value) {
    switch (fd.getType()) {
      case INT32:
      case SINT32:
      case UINT32:
      case FIXED32:
      case SFIXED32:
      case FIXED64:
      case SFIXED64:
      case INT64:
      case SINT64:
      case UINT64:
        return ColumnType.INTEGER;
      case STRING:
        return ColumnType.STRING;
        // FIXME: not recursive type
        // case MESSAGE:
        // case GROUP:
      default:
        throw new IllegalArgumentException(String.format(TYPE_NOT_SUPPORTED_FMT, fd.getType()));
    }
  }

  static LiteralType literalType(FieldDescriptor fd, Object value) {
    switch (fd.getType()) {
      case INT32:
      case SINT32:
      case UINT32:
      case FIXED32:
      case SFIXED32:
      case FIXED64:
      case SFIXED64:
      case INT64:
      case SINT64:
      case UINT64:
        return LiteralType.ofSimpleType(SimpleType.INTEGER);
      case FLOAT:
      case DOUBLE:
        return LiteralType.ofSimpleType(SimpleType.FLOAT);
      case BOOL:
        return LiteralType.ofSimpleType(SimpleType.BOOLEAN);
      case STRING:
        return LiteralType.ofSimpleType(SimpleType.STRING);
      case ENUM:
        return LiteralType.ofSimpleType(SimpleType.STRING);
      case MESSAGE:
      case GROUP:
        return LiteralType.ofSchemaType(schemaType((Message) value));
      default:
        // TODO: throw a proper checked exception
        throw new IllegalArgumentException(String.format(TYPE_NOT_SUPPORTED_FMT, fd.getType()));
    }
  }

  @SuppressWarnings("unchecked")
  public static <T extends Message> T fromLiteralMap(
      Map<String, Literal> literalMap, Class<T> clazz) {
    try {
      T message = (T) clazz.getMethod("getDefaultInstance").invoke(null);
      Map<String, FieldDescriptor> fields =
          message.getDescriptorForType().getFields().stream()
              .collect(Collectors.toMap(FieldDescriptor::getName, identity()));

      Message.Builder builder = message.toBuilder();
      literalMap
          .entrySet()
          .forEach(
              entry -> {
                FieldDescriptor fd = fields.get(entry.getKey());
                builder.setField(fd, fromLiteral(entry.getValue(), fd, builder));
              });

      return message;
    } catch (Exception e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  static Object fromLiteral(Literal literal, FieldDescriptor fd, Message.Builder builder) {
    switch (literal.kind()) {
      case SCALAR:
        return literal.scalar().value();
      case COLLECTION:
        return literal.collection().stream().map(Literal::value).collect(Collectors.toList());
      case MAP:
        if (fd.isMapField()) {
          // FIXME: missing impl
          return null;
        } else {
          return fromLiteral(literal, fd, builder.newBuilderForField(fd));
        }
      default:
        throw new IllegalArgumentException();
    }
  }

  // static Message fromScalar(Scalar scalar, FieldDescriptor fd, Message message) {
  //   switch (scalar.kind()) {
  //     case PRIMITIVE:
  //       return scalar.primitive().value();
  //     default:
  //       throw new IllegalArgumentException();
  //   }
  // }

  // static Object fromPrimitive(Primitive primitive) {
  //   switch (primitive.type()) {
  //     case INTEGER:
  //       return primitive.integer();
  //     default:
  //       throw new IllegalArgumentException();
  //   }
  // }

  @SuppressWarnings("unchecked")
  public static Map<String, Literal> literal(Message message) {
    Map<String, Literal> result = new HashMap<>();

    for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      FieldDescriptor fd = entry.getKey();

      if (fd.isRepeated()) {
        List<Literal> objs =
            ((List<Object>) entry.getValue())
                .stream().map(v -> literal(fd, v)).collect(Collectors.toList());
        result.put(fd.getName(), Literal.ofCollection(objs));
        // }
        // if (fd.isMapField()) {
        // Descriptor type = fd.getMessageType();
        // FieldDescriptor keyField = type.findFieldByName("key");
        // FieldDescriptor valueField = type.findFieldByName("value");
        // // List<Literal> objs = ((List<Object>) entry.getValue()).stream().map(v ->
        // Maps.immutableEntry(literal(fd, v), literal(fd, ))
        // // .collect(Collectors.toList());
        // result.put(fd.getName(), Literal.ofMap(null));
      } else {
        result.put(fd.getName(), literal(fd, entry.getValue()));
      }
    }

    return result;
  }

  static Literal literal(FieldDescriptor fd, Object value) {
    switch (fd.getType()) {
      case INT32:
      case SINT32:
      case UINT32:
      case FIXED32:
      case SFIXED32:
      case FIXED64:
      case SFIXED64:
      case INT64:
      case SINT64:
      case UINT64:
        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofInteger((Long) value)));
      case FLOAT:
      case DOUBLE:
        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofFloat((Double) value)));
      case BOOL:
        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofBoolean((Boolean) value)));
      case STRING:
        return Literal.ofScalar(Scalar.ofPrimitive(Primitive.ofString((String) value)));
      case ENUM:
        return Literal.ofScalar(
            Scalar.ofPrimitive(Primitive.ofString(((EnumValueDescriptor) value).getName())));
      case MESSAGE:
      case GROUP:
        return Literal.ofMap(literal((Message) value));
      default:
        // TODO: throw a proper checked exception
        throw new IllegalArgumentException(String.format(TYPE_NOT_SUPPORTED_FMT, fd.getType()));
    }
  }

  @SuppressWarnings("unchecked")
  public static Map<String, SdkBindingData> bindingData(Message message) {
    Map<String, SdkBindingData> result = new HashMap<>();

    for (Map.Entry<FieldDescriptor, Object> entry : message.getAllFields().entrySet()) {
      FieldDescriptor fd = entry.getKey();

      if (fd.isRepeated()) {
        List<SdkBindingData> objs =
            ((List<Object>) entry.getValue())
                .stream().map(v -> bindingData(fd, v)).collect(Collectors.toList());
        result.put(fd.getName(), SdkBindingData.ofBindingCollection(objs));
        // TODO: implement map fields
        // }
        // if (fd.isMapField()) {
        // Descriptor type = fd.getMessageType();
        // FieldDescriptor keyField = type.findFieldByName("key");
        // FieldDescriptor valueField = type.findFieldByName("value");
        // // List<Literal> objs = ((List<Object>) entry.getValue()).stream().map(v ->
        // Maps.immutableEntry(literal(fd, v), literal(fd, ))
        // // .collect(Collectors.toList());
        // result.put(fd.getName(), Literal.ofMap(null));
      } else {
        result.put(fd.getName(), bindingData(fd, entry.getValue()));
      }
    }

    return result;
  }

  static SdkBindingData bindingData(FieldDescriptor fd, Object value) {
    switch (fd.getType()) {
      case INT32:
      case SINT32:
      case UINT32:
      case FIXED32:
      case SFIXED32:
      case FIXED64:
      case SFIXED64:
      case INT64:
      case SINT64:
      case UINT64:
        return SdkBindingData.ofInteger((Long) value);
      case BOOL:
        return SdkBindingData.ofBoolean((Boolean) value);
      case FLOAT:
      case DOUBLE:
        return SdkBindingData.ofFloat((Double) value);
      case STRING:
        return SdkBindingData.ofString((String) value);
      case ENUM:
        return SdkBindingData.ofString(((EnumValueDescriptor) value).getName());
      case MESSAGE:
      case GROUP:
        return SdkBindingData.ofBindingMap(bindingData((Message) value));
      default:
        // TODO: throw a proper checked exception
        throw new IllegalArgumentException(String.format(TYPE_NOT_SUPPORTED_FMT, fd.getType()));
    }
  }
}
