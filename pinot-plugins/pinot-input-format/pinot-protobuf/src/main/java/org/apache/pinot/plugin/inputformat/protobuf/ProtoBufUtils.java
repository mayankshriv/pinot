/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.protobuf.Descriptors;
import com.google.protobuf.ProtobufInternalUtils;
import java.io.InputStream;
import java.net.URI;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;

public class ProtoBufUtils {
  public static final String TMP_DIR_PREFIX = "pinot-protobuf";
  public static final String PB_OUTER_CLASS_SUFFIX = "OuterClass";

  private ProtoBufUtils() {
  }

  /// Reads the contents of a descriptor file (local or remote) into a byte array. The file is read via
  /// [PinotFS#open] and the stream is closed before returning - no temporary files are created.
  ///
  /// @param descriptorFilePath URI string pointing to a `.desc` protobuf descriptor file
  /// @return the raw bytes of the descriptor file
  public static byte[] readDescriptorFileBytes(String descriptorFilePath)
      throws Exception {
    URI fileURI = URI.create(descriptorFilePath);
    String scheme = fileURI.getScheme();
    if (scheme == null) {
      scheme = PinotFSFactory.LOCAL_PINOT_FS_SCHEME;
    }
    PinotFS pinotFS = PinotFSFactory.create(scheme);
    try (InputStream in = pinotFS.open(fileURI)) {
      return in.readAllBytes();
    }
  }

  public static String getFullJavaName(Descriptors.Descriptor descriptor) {
    String prefix;
    if (null != descriptor.getContainingType()) {
      // nested type
      prefix = getFullJavaName(descriptor.getContainingType());
    } else {
      // top level message
      prefix = getOuterProtoPrefix(descriptor.getFile());
    }
    return prefix + "." + descriptor.getName();
  }

  public static String getFullJavaNameForEnum(Descriptors.EnumDescriptor enumDescriptor) {
    if (null != enumDescriptor.getContainingType()) {
      return getFullJavaName(enumDescriptor.getContainingType())
          + "."
          + enumDescriptor.getName();
    } else {
      String outerProtoName = getOuterProtoPrefix(enumDescriptor.getFile());
      return outerProtoName + "." + enumDescriptor.getName();
    }
  }

  public static String getOuterProtoPrefix(Descriptors.FileDescriptor fileDescriptor) {
    String javaPackageName =
        fileDescriptor.getOptions().hasJavaPackage()
            ? fileDescriptor.getOptions().getJavaPackage()
            : fileDescriptor.getPackage();
    if (fileDescriptor.getOptions().getJavaMultipleFiles()) {
      return javaPackageName;
    } else if (fileDescriptor.getOptions().hasJavaOuterClassname()) {
      return javaPackageName + "." + fileDescriptor.getOptions().hasJavaOuterClassname();
    } else {
      String[] fileNames = fileDescriptor.getName().split("/");
      String fileName = fileNames[fileNames.length - 1];
      String outerName = ProtobufInternalUtils.underScoreToCamelCase(fileName.split("\\.")[0], true);
      if (hasTypeWithName(fileDescriptor.getMessageTypes(), outerName)
          || hasTypeWithName(fileDescriptor.getEnumTypes(), outerName)
          || hasTypeWithName(fileDescriptor.getServices(), outerName)) {
        // https://developers.google.com/protocol-buffers/docs/reference/java-generated#invocation
        // The name of the wrapper class is determined by converting the base name of the .proto
        // file to camel case if the java_outer_classname option is not specified.
        // For example, foo_bar.proto produces the class name FooBar. If there is a service,
        // enum, or message (including nested types) in the file with the same name,
        // "OuterClass" will be appended to the wrapper class's name.
        return javaPackageName + "." + outerName + PB_OUTER_CLASS_SUFFIX;
      } else {
        return javaPackageName + "." + outerName;
      }
    }
  }

  private static boolean hasTypeWithName(Iterable<? extends Descriptors.GenericDescriptor> descriptors, String name) {
    for (Descriptors.GenericDescriptor descriptor : descriptors) {
      if (descriptor.getName().equals(name)) {
        return true;
      }
    }
    return false;
  }

  /// Get java type str from [Descriptors.FieldDescriptor] which directly fetched from protobuf object.
  ///
  /// @return The returned code phrase will be used as java type str in codegen sections.
  public static String getTypeStrFromProto(Descriptors.FieldDescriptor desc) {
    switch (desc.getJavaType()) {
      case INT:
        return "Integer";
      case LONG:
        return "Long";
      case STRING:
        return "String";
      case FLOAT:
        return "Float";
      case DOUBLE:
        return "Double";
      case BYTE_STRING:
        return "ByteString";
      case BOOLEAN:
        return "Boolean";
      case ENUM:
        return getFullJavaNameForEnum(desc.getEnumType());
      case MESSAGE:
        if (desc.isMapField()) {
          // map
          final Descriptors.FieldDescriptor key = desc.getMessageType().findFieldByName("key");
          final Descriptors.FieldDescriptor value = desc.getMessageType().findFieldByName("value");
          // key and value cannot be repeated
          String keyTypeStr = getTypeStrFromProto(key);
          String valueTypeStr = getTypeStrFromProto(value);
          return "Map<" + keyTypeStr + "," + valueTypeStr + ">";
        } else {
          // simple message
          return getFullJavaName(desc.getMessageType());
        }
      default:
        throw new RuntimeException("do not support field type: " + desc.getJavaType());
    }
  }
}
