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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufCodeGenMessageDecoder.PROTOBUF_JAR_FILE_PATH;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufCodeGenMessageDecoder.PROTO_CLASS_NAME;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.createComplexTypeRecord;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.getComplexTypeObject;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.getFieldsInSampleRecord;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.getSampleRecordMessage;
import static org.apache.pinot.plugin.inputformat.protobuf.ProtoBufTestDataGenerator.getSourceFieldsForComplexType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Verifies that protobuf decoder and reader operations do not leak temporary directories.
///
/// Each test snapshots the set of `pinot-protobuf*` directories in the system temp directory before the operation,
/// then asserts that no new ones remain afterward. The functional assertions confirm the operation itself still works
/// correctly.
public class ProtoBufTempFileLeakTest {
  private static final Path TEMP_DIR = Path.of(System.getProperty("java.io.tmpdir"));

  /// [ProtoBufMessageDecoder#init] with a descriptor file should not leak a temp directory.
  /// This is the streaming consumer path - called once per consumer init.
  @Test
  public void testMessageDecoderInitDoesNotLeakTempDir()
      throws Exception {
    Set<Path> before = listProtobufTempDirs();

    Map<String, String> decoderProps = new HashMap<>();
    URL descriptorFile = getClass().getClassLoader().getResource("sample.desc");
    decoderProps.put("descriptorFile", descriptorFile.toURI().toString());
    ProtoBufMessageDecoder decoder = new ProtoBufMessageDecoder();
    decoder.init(decoderProps, getFieldsInSampleRecord(), "");

    // Verify functional correctness - decoding still works
    Sample.SampleRecord sampleRecord = getSampleRecordMessage();
    GenericRow destination = new GenericRow();
    decoder.decode(sampleRecord.toByteArray(), destination);
    assertEquals(destination.getValue("email"), "foobar@hello.com");
    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("id"), 18);

    assertNoNewTempDirs(before);
  }

  /// [ProtoBufMessageDecoder#init] with a complex descriptor should not leak a temp directory.
  @Test
  public void testMessageDecoderComplexDescriptorDoesNotLeakTempDir()
      throws Exception {
    Set<Path> before = listProtobufTempDirs();

    Map<String, String> decoderProps = new HashMap<>();
    URL descriptorFile = getClass().getClassLoader().getResource("complex_types.desc");
    decoderProps.put("descriptorFile", descriptorFile.toURI().toString());
    ProtoBufMessageDecoder decoder = new ProtoBufMessageDecoder();
    decoder.init(decoderProps, getSourceFieldsForComplexType(), "");

    // Verify functional correctness
    Map<String, Object> inputRecord = createComplexTypeRecord();
    GenericRow destination = new GenericRow();
    decoder.decode(getComplexTypeObject(inputRecord).toByteArray(), destination);
    assertNotNull(destination.getValue("string_field"));
    assertEquals(destination.getValue("string_field"), "hello");

    assertNoNewTempDirs(before);
  }

  /// [ProtoBufCodeGenMessageDecoder#init] with a JAR file should not leak a temp directory.
  /// This is the streaming consumer codegen path.
  @Test
  public void testCodeGenDecoderInitDoesNotLeakTempDir()
      throws Exception {
    Set<Path> before = listProtobufTempDirs();

    Map<String, String> decoderProps = new HashMap<>();
    URL jarFile = getClass().getClassLoader().getResource("sample.jar");
    decoderProps.put(PROTOBUF_JAR_FILE_PATH, jarFile.toURI().toString());
    decoderProps.put(PROTO_CLASS_NAME,
        "org.apache.pinot.plugin.inputformat.protobuf.Sample$SampleRecord");
    ProtoBufCodeGenMessageDecoder decoder = new ProtoBufCodeGenMessageDecoder();
    decoder.init(decoderProps, getFieldsInSampleRecord(), "");

    // Verify functional correctness - decoding still works after cleanup
    Sample.SampleRecord sampleRecord = getSampleRecordMessage();
    GenericRow destination = new GenericRow();
    decoder.decode(sampleRecord.toByteArray(), destination);
    assertEquals(destination.getValue("email"), "foobar@hello.com");
    assertEquals(destination.getValue("name"), "Alice");
    assertEquals(destination.getValue("id"), 18);

    assertNoNewTempDirs(before);
  }

  /// [ProtoBufCodeGenMessageDecoder#init] with a complex JAR should not leak a temp directory,
  /// and decoding complex/nested types must still work after cleanup.
  @Test
  public void testCodeGenDecoderComplexJarDoesNotLeakTempDir()
      throws Exception {
    Set<Path> before = listProtobufTempDirs();

    Map<String, String> decoderProps = new HashMap<>();
    URL jarFile = getClass().getClassLoader().getResource("complex_types.jar");
    decoderProps.put(PROTOBUF_JAR_FILE_PATH, jarFile.toURI().toString());
    decoderProps.put(PROTO_CLASS_NAME,
        "org.apache.pinot.plugin.inputformat.protobuf.ComplexTypes$TestMessage");
    ProtoBufCodeGenMessageDecoder decoder = new ProtoBufCodeGenMessageDecoder();
    decoder.init(decoderProps, getSourceFieldsForComplexType(), "");

    // Verify functional correctness
    Map<String, Object> inputRecord = createComplexTypeRecord();
    GenericRow destination = new GenericRow();
    decoder.decode(getComplexTypeObject(inputRecord).toByteArray(), destination);
    assertNotNull(destination.getValue("string_field"));
    assertEquals(destination.getValue("string_field"), "hello");

    assertNoNewTempDirs(before);
  }

  /// [ProtoBufRecordReader] lifecycle (init, read, close) should not leak a temp directory.
  /// This is the batch ingestion path.
  @Test
  public void testRecordReaderLifecycleDoesNotLeakTempDir()
      throws Exception {
    Set<Path> before = listProtobufTempDirs();

    // Write a small protobuf data file
    File tempDataDir = Files.createTempDirectory("protobuf-leak-test-data").toFile();
    File dataFile = new File(tempDataDir, "test.data");
    try {
      try (FileOutputStream out = new FileOutputStream(dataFile)) {
        getSampleRecordMessage().writeDelimitedTo(out);
      }

      URL descriptorFile = getClass().getClassLoader().getResource("sample.desc");
      ProtoBufRecordReaderConfig config = new ProtoBufRecordReaderConfig();
      config.setDescriptorFile(descriptorFile.toURI());

      try (ProtoBufRecordReader reader = new ProtoBufRecordReader()) {
        reader.init(dataFile, getFieldsInSampleRecord(), config);

        // Verify functional correctness
        assertTrue(reader.hasNext());
        GenericRow row = reader.next(new GenericRow());
        assertEquals(row.getValue("email"), "foobar@hello.com");
        assertEquals(row.getValue("name"), "Alice");
      }
    } finally {
      //noinspection ResultOfMethodCallIgnored
      dataFile.delete();
      //noinspection ResultOfMethodCallIgnored
      tempDataDir.delete();
    }

    assertNoNewTempDirs(before);
  }

  private static void assertNoNewTempDirs(Set<Path> before)
      throws IOException {
    Set<Path> after = listProtobufTempDirs();
    Set<Path> leaked = after.stream()
        .filter(p -> !before.contains(p))
        .collect(Collectors.toSet());
    assertTrue(leaked.isEmpty(),
        "Leaked " + leaked.size() + " temp director" + (leaked.size() == 1 ? "y" : "ies") + ": " + leaked);
  }

  private static Set<Path> listProtobufTempDirs()
      throws IOException {
    if (!Files.isDirectory(TEMP_DIR)) {
      return Set.of();
    }
    try (Stream<Path> entries = Files.list(TEMP_DIR)) {
      return entries
          .filter(p -> p.getFileName().toString().startsWith(ProtoBufUtils.TMP_DIR_PREFIX))
          .collect(Collectors.toSet());
    }
  }
}
