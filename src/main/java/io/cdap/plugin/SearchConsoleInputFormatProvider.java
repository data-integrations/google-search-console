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

import com.google.common.collect.ImmutableMap;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.batch.InputFormatProvider;

import java.util.Map;

/**
 * InputFormatProvider used by cdap to provide configurations to mapreduce job
 */
public class SearchConsoleInputFormatProvider implements InputFormatProvider {

  private final Map<String, String> conf;
  private static final Gson GSON = new GsonBuilder().create();

  SearchConsoleInputFormatProvider(SearchConsoleSourceConfig config) {
    this.conf = new ImmutableMap.Builder<String, String>()
      .put(SearchConsoleConstants.CONFIGURATION_PROPERTY_NAME,
           GSON.toJson(config.getProperties()))
      .build();
  }

  @Override
  public String getInputFormatClassName() {
    return SearchConsoleInputFormat.class.getName();
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return conf;
  }
}
