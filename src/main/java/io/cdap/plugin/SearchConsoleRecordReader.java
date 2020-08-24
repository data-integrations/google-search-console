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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.webmasters.Webmasters;
import com.google.api.services.webmasters.Webmasters.Searchanalytics.Query;
import com.google.api.services.webmasters.model.ApiDataRow;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryRequest;
import com.google.api.services.webmasters.model.SearchAnalyticsQueryResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * RecordReader implementation, which reads {@link ApiDataRow} and http codes from search console
 * api.
 */
public class SearchConsoleRecordReader extends RecordReader<NullWritable, ApiDataRow> {

  private static final Logger LOG = LoggerFactory.getLogger(SearchConsoleRecordReader.class);

  private Iterator<ApiDataRow> recordIterator;
  private ApiDataRow value;
  private SearchConsoleSourceConfig searchConsoleSourceConfig;
  private SearchConsoleQuery queryConfig;

  @Override
  public void initialize(final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext)
    throws IOException {
    Configuration conf = taskAttemptContext.getConfiguration();
    queryConfig = ((SearchConsoleSplit) inputSplit).getQuery();
    this.searchConsoleSourceConfig = SearchConsoleUtils.extractPropertiesFromConfig(conf);
    recordIterator = getQueryData().iterator();
  }

  List<ApiDataRow> getQueryData() throws IOException {
    GoogleCredential googleCredential = SearchConsoleUtils.generateCredential(this.searchConsoleSourceConfig);
    Webmasters service = SearchConsoleUtils.generateService(googleCredential);
    SearchAnalyticsQueryRequest searchAnalyticsQueryRequest = new SearchAnalyticsQueryRequest();
    searchAnalyticsQueryRequest.setStartDate(searchConsoleSourceConfig.getStartDate());
    searchAnalyticsQueryRequest.setEndDate(searchConsoleSourceConfig.getEndDate());
    searchAnalyticsQueryRequest.setDimensions(searchConsoleSourceConfig.getDimensions());
    searchAnalyticsQueryRequest.setRowLimit(SearchConsoleConstants.SEARCH_CONSOLE_MAX_ROW_LIMIT);
    List<ApiDataRow> rows = new ArrayList<>();
    for (String site : queryConfig.getSites()) {
      LOG.info("Fetching: " + site);
      String siteType = site.startsWith(SearchConsoleConstants.SEARCH_CONSOLE_DOMAIN_PREFIX)
        ? SearchConsoleConstants.SITE_TYPE_DOMAIN : SearchConsoleConstants.SITE_TYPE_URL_PREFIX;
      rows.addAll(executeQuerySite(service, searchAnalyticsQueryRequest, site, siteType));
    }
    return rows;
  }


  private List<ApiDataRow> executeQuerySite(Webmasters service, SearchAnalyticsQueryRequest request,
                                            String site, String siteType) {
    List<ApiDataRow> rows = new ArrayList<>();
    int i = 0;
    boolean hasRecords;
    do {
      try {
        request.setStartRow(i * SearchConsoleConstants.SEARCH_CONSOLE_MAX_ROW_LIMIT);
        Query query = service.searchanalytics().query(site, request);
        SearchAnalyticsQueryResponse response = query.execute();
        i++;
        hasRecords = Optional.ofNullable(response.getRows()).isPresent() && response.getRows().size() != 0;
        if (hasRecords) {
          rows.addAll(addSiteInfo(response.getRows(), site, siteType));
        }
      } catch (GoogleJsonResponseException ex) {
        hasRecords = false;
        LOG.error("Failed to fetch site: {} - {}", site, ex.getDetails().getMessage());
      } catch (IOException ex) {
        hasRecords = false;
        LOG.error("Failed to fetch site: {} - {}", site, ex.getMessage());
      }
    } while (hasRecords); // Page through all result rows
    return rows;
  }

  private List<ApiDataRow> addSiteInfo(List<ApiDataRow> rows, String site, String siteType) {
    return rows.stream().peek(row -> {
      row.set("site", site);
      row.set("type", siteType);
    }).collect(Collectors.toList());
  }

  @Override
  public boolean nextKeyValue() {
    if (!recordIterator.hasNext()) {
      return false;
    }
    value = recordIterator.next();
    return true;
  }

  @Override
  public NullWritable getCurrentKey() {
    return null;
  }

  @Override
  public ApiDataRow getCurrentValue() {
    return value;
  }

  @Override
  public float getProgress() {
    return 0.0f;
  }

  @Override
  public void close() {
  }
}
