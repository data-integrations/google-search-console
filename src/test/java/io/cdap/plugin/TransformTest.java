/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin;


import com.google.api.services.webmasters.model.ApiDataRow;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.TestConfiguration;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import static org.powermock.api.mockito.PowerMockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Unit tests for our plugins.
 */
public class TransformTest extends HydratorTestBase {

  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("data-pipeline", "1.0.0");

  private static final String[] KEYS = new String[]{"country", "device"};
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT
        .artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
        parentArtifact,
        SearchConsoleSource.class
    );
  }

  /**
   * Test transform operation by mocking search console api results
   *
   * @throws Exception when test fails
   */
  @Test
  @PrepareForTest(SearchConsoleSource.class)
  public void testSearchConsoleSource() throws Exception {
    BatchRuntimeContext mock = Mockito.mock(BatchRuntimeContext.class);
    List<ApiDataRow> apiDataRows = generateRows();

    SearchConsoleSourceConfig consoleSourceConfig = SearchConsoleSourceConfigHelper
        .newConfigBuilder().
            build();
    SearchConsoleRecordReader searchConsoleRecordReader = PowerMockito
        .mock(SearchConsoleRecordReader.class);
    when(searchConsoleRecordReader.getQueryData()).thenReturn(apiDataRows);
    PowerMockito.whenNew(SearchConsoleRecordReader.class).withAnyArguments()
        .thenReturn(searchConsoleRecordReader);

    ApiDataRow apiDataRow = apiDataRows.get(0);
    SearchConsoleSource searchConsoleSource = new SearchConsoleSource(consoleSourceConfig);
    searchConsoleSource.initialize(mock);
    Emitter<StructuredRecord> emitter = new Emitter<StructuredRecord>() {
      @Override
      public void emit(StructuredRecord record) {
        Assert.assertEquals(apiDataRow.getClicks(), record.get("clicks"));
      }

      @Override
      public void emitAlert(Map<String, String> map) {

      }

      @Override
      public void emitError(InvalidEntry<StructuredRecord> invalidEntry) {

      }
    };

    KeyValue<String, ApiDataRow> temp = new KeyValue<>("temp", apiDataRow);
    searchConsoleSource.transform(temp, emitter);
  }

  private List<ApiDataRow> generateRows() {
    List<ApiDataRow> list = new ArrayList<>();
    ApiDataRow dataRow = new ApiDataRow();
    dataRow.setClicks(Math.random());
    dataRow.setImpressions(Math.random());
    dataRow.setCtr(Math.random());
    dataRow.setPosition(Math.random());
    dataRow.setKeys(Arrays.asList(SearchConsoleSourceConfigHelper.TEST_DIMENSIONS.split(",")));
    list.add(dataRow);
    return list;
  }

}
