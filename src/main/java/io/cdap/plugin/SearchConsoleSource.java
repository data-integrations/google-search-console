/*
 * Copyright © 2020 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class <code>SearchConsoleSource</code> is a plugin that would allow users
 * to read analytics data from Google Search Console API
 * <p>
 * This plugin fetches and transforms <code>ApiDataRows</code> to <code>StructuredRecords</code>.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SearchConsoleSource.NAME)
@Description("Reads from a FileSet that has its data formatted as text.")
public class SearchConsoleSource extends BatchSource<String, ApiDataRow, StructuredRecord> {

  public static final String NAME = "SearchConsoleSource";
  private final SearchConsoleSourceConfig config;

  public SearchConsoleSource(SearchConsoleSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws IOException {
    emitLineage(context, config.getParsedSchema());
    context.setInput(Input.of(config.referenceName, new SearchConsoleInputFormatProvider(config)));
  }

  /**
   * Transforms ApiDataRow into StructuredRecord
   *
   * @param input   link{ApiDataRow}
   * @param emitter link{StructuredRecord}
   * @throws Exception when transform operation fails
   */
  @Override
  public void transform(KeyValue<String, ApiDataRow> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    Schema outputSchema = config.getParsedSchema();
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    ApiDataRow row = input.getValue();
    mapDimensionKeys(row, builder, outputSchema);
    mapDimensionValues(row, builder, outputSchema);
    StructuredRecord record = builder.build();
    emitter.emit(record);
  }

  /**
   * Maps dimensions array from link{ApiDataRow} into link{StructuredRecord}
   *
   * @param row          link{ApiDataRow}
   * @param builder      link{StructuredRecord.Builder}
   * @param outputSchema link{Schema}
   * @throws IOException is raised if field cannot be converted
   */
  private void mapDimensionValues(ApiDataRow row, StructuredRecord.Builder builder, Schema outputSchema)
    throws IOException {
    for (Schema.Field field : outputSchema.getFields()) {
      if (Optional.ofNullable(row.get(field.getName())).isPresent()) {
        builder.set(field.getName(),
                    SearchConsoleUtils.convertField(row.get(field.getName()), field.getSchema()));
      }
    }
  }

  /**
   * Map dimension fields to StructuredRecord schema
   *
   * @param row          {@link ApiDataRow}
   * @param builder      {@link StructuredRecord}
   * @param outputSchema {@link Schema}
   * @throws IOException is raised if field cannot be converted
   */
  private void mapDimensionKeys(ApiDataRow row, StructuredRecord.Builder builder, Schema outputSchema)
    throws IOException {
    List<String> dimensions = config.getDimensions();
    for (int i = 0; i < dimensions.size(); i++) {
      if (Optional.ofNullable(outputSchema.getField(dimensions.get(i))).isPresent()) {
        Schema.Field field = outputSchema.getField(dimensions.get(i));
        Object value = ((List) row.get(SearchConsoleConstants.DIMENSIONS_PROPERTY_NAME)).get(i);
        builder.set(dimensions.get(i), SearchConsoleUtils.convertField(value, field.getSchema()));
      }
    }
  }

  /**
   * Generates lineage for Search Console API
   *
   * @param context {@link BatchSourceContext}
   * @param schema  {@link Schema}
   */
  private void emitLineage(BatchSourceContext context, Schema schema) {
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.referenceName);
    lineageRecorder.createExternalDataset(schema);

    if (schema.getFields() != null) {
      lineageRecorder.recordRead("Read", "Read from Search Console API.",
                                 schema.getFields().stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }
}
