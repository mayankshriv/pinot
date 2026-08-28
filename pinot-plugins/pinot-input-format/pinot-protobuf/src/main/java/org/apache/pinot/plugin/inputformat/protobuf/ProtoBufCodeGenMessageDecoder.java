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

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.plugin.inputformat.protobuf.codegen.MessageCodeGen;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Protobuf stream decoder that uses Janino-compiled code for extraction. The generated code is compiled at
/// init time from the protobuf descriptor found in the user-supplied JAR. The [URLClassLoader] and its backing
/// JAR file must remain available for the lifetime of this decoder because the JVM may lazily resolve classes
/// referenced by the generated code at decode time. Since [StreamMessageDecoder] does not extend
/// [java.io.Closeable], these resources are not explicitly released.
public class ProtoBufCodeGenMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ProtoBufCodeGenMessageDecoder.class);

  public static final String PROTOBUF_JAR_FILE_PATH = "jarFile";
  public static final String PROTO_CLASS_NAME = "protoClassName";
  private Method _decodeMethod;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    Preconditions.checkState(props.containsKey(PROTOBUF_JAR_FILE_PATH),
        "Protocol Buffer schema jar file must be provided");
    Preconditions.checkState(props.containsKey(PROTO_CLASS_NAME),
        "Protocol Buffer Message class name must be provided");
    String protoClassName = props.getOrDefault(PROTO_CLASS_NAME, "");
    String jarPath = props.getOrDefault(PROTOBUF_JAR_FILE_PATH, "");
    File jarFile = resolveToLocalFile(jarPath);
    ClassLoader protoMessageClsLoader = createClassLoader(jarFile);
    Descriptors.Descriptor descriptor = getDescriptorForProtoClass(protoMessageClsLoader, protoClassName);
    String codeGenCode = new MessageCodeGen().codegen(descriptor, fieldsToRead);
    Class<?> recordExtractor = compileClass(protoMessageClsLoader,
        MessageCodeGen.EXTRACTOR_PACKAGE_NAME + "." + MessageCodeGen.EXTRACTOR_CLASS_NAME, codeGenCode);
    _decodeMethod = recordExtractor.getMethod(MessageCodeGen.EXTRACTOR_METHOD_NAME, byte[].class, GenericRow.class);
    // NOTE: Do NOT close the URLClassLoader or delete the JAR file. The generated code may trigger
    // lazy class resolution at decode time via the classloader chain (Janino -> URLClassLoader -> JAR).
    // Closing prematurely would cause NoClassDefFoundError. For local JARs there is nothing to
    // clean up. For remote JARs, the temp directory persists for the lifetime of this decoder
    // (StreamMessageDecoder does not extend Closeable).
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      destination = (GenericRow) _decodeMethod.invoke(null, payload, destination);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while decoding protobuf message", e);
    }
    return destination;
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    if (offset != 0 || payload.length > length) {
      payload = Arrays.copyOfRange(payload, offset, offset + length);
    }
    return decode(payload, destination);
  }

  public static ClassLoader createClassLoader(File jarFile) {
    try {
      URL url = jarFile.toURI().toURL();
      return new URLClassLoader(new URL[]{url});
    } catch (Exception e) {
      throw new RuntimeException("Error loading protobuf class", e);
    }
  }

  public static Class<?> compileClass(ClassLoader classloader, String className, String code)
      throws ClassNotFoundException {
    SimpleCompiler simpleCompiler = new SimpleCompiler();
    simpleCompiler.setParentClassLoader(classloader);
    try {
      simpleCompiler.cook(code);
    } catch (Throwable t) {
      throw new RuntimeException("Program cannot be compiled. This is a bug. Please file an issue.", t);
    }
    return simpleCompiler.getClassLoader().loadClass(className);
  }

  public static Descriptors.Descriptor getDescriptorForProtoClass(ClassLoader protoMessageClsLoader,
      String protoClassName)
      throws NoSuchMethodException, ClassNotFoundException, InvocationTargetException, IllegalAccessException {
    Class<? extends Message> updateMessage = (Class<Message>) protoMessageClsLoader.loadClass(protoClassName);
    return (Descriptors.Descriptor) updateMessage.getMethod("getDescriptor").invoke(null);
  }

  /// Resolves a file path (URI string) to a local [File]. For local files (no scheme or `file://` scheme),
  /// the original file is returned directly. For remote files, the file is copied to a local temporary
  /// directory. The caller is responsible for the lifetime of the returned file - for remote files, the
  /// backing temp directory is intentionally NOT cleaned up because the JAR must remain accessible for
  /// lazy class loading at decode time.
  private static File resolveToLocalFile(String filePath)
      throws Exception {
    URI fileURI = URI.create(filePath);
    String scheme = fileURI.getScheme();
    if (scheme == null || PinotFSFactory.LOCAL_PINOT_FS_SCHEME.equals(scheme)) {
      return new File(fileURI.getPath());
    }
    PinotFS pinotFS = PinotFSFactory.create(scheme);
    Path localTmpDir = Files.createTempDirectory(ProtoBufUtils.TMP_DIR_PREFIX);
    File localFile = new File(localTmpDir.toFile(), new File(fileURI.getPath()).getName());
    LOGGER.info("Copying protocol buffer JAR from {} to {}", filePath, localFile.getAbsolutePath());
    pinotFS.copyToLocalFile(fileURI, localFile);
    return localFile;
  }
}
