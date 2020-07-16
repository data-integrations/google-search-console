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

/**
 * Helper for tests
 */
public class SearchConsoleSourceConfigHelper {

  public static final String TEST_REF_NAME = "TestRefName";
  public static final String TEST_PROJECT = "test-project";
  public static final String TEST_NAMESPACE = "TestNamespace";
  public static final String TEST_FILE_PATH = "/path/to/file";
  public static final int TEST_NUM_SPLITS = 1;
  public static final String TEST_AUTH_METHOD = "serviceAccountJSON";
  public static final String TEST_CLIENT_ID = "clientId";
  public static final String TEST_CLIENT_SECRET = "clientSecret";
  public static final String TEST_ACCESS_TOKEN = "accessToken";
  public static final String TEST_APPLICATION_NAME = "applicationName";
  public static final String TEST_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
  public static final String TEST_FILTER_OPTION = "allUrlsOption";
  public static final String TEST_FILTER_OPTION_LIST = "siteUrlListOption";
  public static final String TEST_SITE_URL_LIST = "Domain__SITE_KV_DELIMITER__example";
  public static final String TEST_START_DATE = "2020-05-05";
  public static final String TEST_END_DATE = "2020-05-06";
  public static final String TEST_DIMENSIONS = "country, device";
  public static final String TEST_SCHEMA = "{\"type\":\"record\",\"name\":\"etlSchemaBody\","
      + "\"fields\":[{\"name\":\"country\",\"type\":\"string\"},{\"name\":\"device\","
      + "\"type\":\"string\"},{\"name\":\"clicks\",\"type\":\"double\"},{\"name\":"
      + "\"impressions\",\"type\":\"string\"},{\"name\":\"ctr\",\"type\":\"string\"},"
      + "{\"name\":\"position\",\"type\":\"string\"}]}";

  public static SearchConsoleSourceConfigBuilder newConfigBuilder() {
    return new SearchConsoleSourceConfigBuilder();
  }

  public static class SearchConsoleSourceConfigBuilder {

    private String referenceName = TEST_REF_NAME;
    private String serviceFilePath = TEST_FILE_PATH;
    private int numSplits = TEST_NUM_SPLITS;
    private String authenticationMethod = TEST_AUTH_METHOD;
    private String clientId = TEST_CLIENT_ID;
    protected String clientSecret = TEST_CLIENT_SECRET;
    protected String clientAccessToken = TEST_ACCESS_TOKEN;
    protected String clientApplicationName = TEST_APPLICATION_NAME;
    protected String serviceAccountJson = TEST_SERVICE_ACCOUNT_JSON;
    private String sitesFilterOption = TEST_FILTER_OPTION;
    private String siteUrlList = TEST_SITE_URL_LIST;
    private String startDate = TEST_START_DATE;
    private String endDate = TEST_END_DATE;
    private String dimensions = TEST_DIMENSIONS;
    private String schema = TEST_SCHEMA;
    public static final String OAUTH_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly";
    private static final String ACCEPTED_DATE_FORMAT = "YYYY-MM-DD";

    public SearchConsoleSourceConfigBuilder setReferenceName(String referenceName) {
      this.referenceName = referenceName;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setServiceFilePath(String serviceFilePath) {
      this.serviceFilePath = serviceFilePath;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setNumSplits(int numSplits) {
      this.numSplits = numSplits;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setAuthenticationMethod(String authenticationMethod) {
      this.authenticationMethod = authenticationMethod;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setClientId(String clientId) {
      this.clientId = clientId;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setClientSecret(String clientSecret) {
      this.clientSecret = clientSecret;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setClientAccessToken(String clientAccessToken) {
      this.clientAccessToken = clientAccessToken;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setClientApplicationName(String clientApplicationName) {
      this.clientApplicationName = clientApplicationName;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setServiceAccountJson(String serviceAccountJson) {
      this.serviceAccountJson = serviceAccountJson;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setSitesFilterOption(String sitesFilterOption) {
      this.sitesFilterOption = sitesFilterOption;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setSiteUrlList(String siteUrlList) {
      this.siteUrlList = siteUrlList;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setStartDate(String startDate) {
      this.startDate = startDate;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setEndDate(String endDate) {
      this.endDate = endDate;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setDimensions(String dimensions) {
      this.dimensions = dimensions;
      return this;
    }

    public SearchConsoleSourceConfigBuilder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public SearchConsoleSourceConfig build() {
      return new SearchConsoleSourceConfig(referenceName, sitesFilterOption,
          siteUrlList, startDate, endDate, dimensions,
          schema, clientId, clientSecret, clientAccessToken, clientApplicationName, serviceFilePath,
          serviceAccountJson, numSplits, authenticationMethod);
    }
  }

}
