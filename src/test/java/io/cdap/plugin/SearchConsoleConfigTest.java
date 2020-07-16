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

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SearchConsoleConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGetAuthenticationMethod() {
    SearchConsoleSourceConfig config = SearchConsoleSourceConfigHelper.newConfigBuilder()
        .setAuthenticationMethod(SearchConsoleSourceConfigHelper.TEST_AUTH_METHOD)
        .build();

    Assert.assertEquals(SearchConsoleSourceConfigHelper.TEST_AUTH_METHOD,
        config.getAuthenticationMethod());
    config.setAuthenticationMethod(null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testDateConfig() {
    SearchConsoleSourceConfig config = SearchConsoleSourceConfigHelper.newConfigBuilder()
        .build();

    Assert.assertEquals(SearchConsoleSourceConfigHelper.TEST_START_DATE, config.getStartDate());
    Assert.assertEquals(SearchConsoleSourceConfigHelper.TEST_END_DATE, config.getEndDate());
  }

  @Test
  public void testStartDateConfig() {
    SearchConsoleSourceConfig config = SearchConsoleSourceConfigHelper.newConfigBuilder()
        .build();

    Assert.assertEquals(SearchConsoleSourceConfigHelper.TEST_START_DATE, config.getStartDate());

    MockFailureCollector collector = new MockFailureCollector();
    // start date
    config.setStartDate(null);
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    config.setStartDate(SearchConsoleSourceConfigHelper.TEST_START_DATE);
  }

  @Test
  public void testEndDateConfig() {
    SearchConsoleSourceConfig config = SearchConsoleSourceConfigHelper.newConfigBuilder()
        .build();

    Assert.assertEquals(SearchConsoleSourceConfigHelper.TEST_END_DATE, config.getEndDate());

    MockFailureCollector collector = new MockFailureCollector();

    //end date
    config.setEndDate(null);
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

  @Test
  public void testGetSiteUrlList() {
    SearchConsoleSourceConfig config = SearchConsoleSourceConfigHelper.newConfigBuilder()
        .setSitesFilterOption(SearchConsoleSourceConfigHelper.TEST_FILTER_OPTION_LIST)
        .build();

    Assert.assertEquals(SearchConsoleSourceConfigHelper.TEST_SITE_URL_LIST,
        config.getSiteUrlList());
    Assert.assertEquals(1, config.getSiteEntries().size());
    config.setSitesUrlList(null);
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
  }

}
