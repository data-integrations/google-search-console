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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * InputFormat for mapreduce job, which provides split of data per site.
 */
public class SearchConsoleInputFormat extends InputFormat<NullWritable, ApiDataRow> {

  @Override
  public List<InputSplit> getSplits(final JobContext jobContext)
    throws IOException {
    final Configuration configuration = jobContext.getConfiguration();
    SearchConsoleSourceConfig searchConsoleSourceConfig = SearchConsoleUtils
      .extractPropertiesFromConfig(configuration);

    List<String> sitesUrls = new ArrayList<>(SearchConsoleUtils.getSitesUrls(searchConsoleSourceConfig));
    final int totalSites = sitesUrls.size();
    final int numSplits = Math.min(searchConsoleSourceConfig.getNumSplits(), totalSites);
    final int perPage = (int) Math.ceil((double) totalSites / (double) numSplits);

    List<InputSplit> splits = new ArrayList<>();
    final AtomicInteger counter = new AtomicInteger();
    final Collection<List<String>> result = sitesUrls.stream()
      .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / perPage))
      .values();

    result.forEach(siteGroup -> splits.add(generateSplit(siteGroup)));

    return splits;
  }

  /**
   * Generate {@link SearchConsoleSplit} from list of sites
   *
   * @param sites {@link List<String>} list of sites (with prefix included)
   * @return {@link SearchConsoleSplit}
   */
  private SearchConsoleSplit generateSplit(List<String> sites) {
    return new SearchConsoleSplit(new SearchConsoleQuery(sites));
  }

  @Override
  public RecordReader<NullWritable, ApiDataRow> createRecordReader(final InputSplit inputSplit,
                                                                   final TaskAttemptContext taskAttemptContext) {
    return new SearchConsoleRecordReader();
  }
}
