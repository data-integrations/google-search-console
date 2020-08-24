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

import com.google.api.services.webmasters.Webmasters;
import com.google.common.base.Strings;

import com.google.gson.JsonObject;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

/**
 * Configuration class for search console source
 */
public class SearchConsoleSourceConfig extends ReferencePluginConfig {

  public static final String PROPERTY_REFERENCE_NAME = "referenceName";
  public static final String AUTHENTICATION_METHOD = "authenticationMethod";
  public static final String CLIENT_ID = "clientId";
  public static final String CLIENT_SECRET = "clientSecret";
  public static final String CLIENT_ACCESS_TOKEN = "clientAccessToken";
  public static final String CLIENT_ACCESS_APPLICATION_NAME = "clientApplicationName";
  public static final String SERVICE_ACCOUNT_FILE = "serviceFilePath";
  public static final String SERVICE_ACCOUNT_JSON = "serviceAccountJsonStr";
  public static final String SITES_FILTER_OPTION = "siteFilterOption";
  public static final String SITES_FILTERED_LIST = "siteUrlList";
  public static final String START_DATE = "startDate";
  public static final String END_DATE = "endDate";
  public static final String DIMENSIONS = "dimensions";
  public static final String NUM_SPLITS = "numSplits";
  public static final String SCHEMA = "SCHEMA";
  public static final String AUTO_DETECT = "auto-detect";
  public static final String AUTHENTICATION_METHOD_OAUTH_CLIENT_ID = "oAuthClient";
  public static final String OAUTH_SCOPE = "https://www.googleapis.com/auth/webmasters.readonly";
  private static final String ACCEPTED_DATE_FORMAT = "YYYY-MM-DD";

  @Macro
  @Name(AUTHENTICATION_METHOD)
  @Description("Authentication method. Defaults to OAuth Client ID.")
  private String authenticationMethod;

  @Macro
  @Name(CLIENT_ID)
  @Nullable
  @Description("Search Console Client ID")
  private String clientId;

  @Macro
  @Name(CLIENT_SECRET)
  @Nullable
  @Description("Search Console Client Secret")
  protected String clientSecret;

  @Macro
  @Name(CLIENT_ACCESS_TOKEN)
  @Nullable
  @Description("Search Console Client Access Token")
  protected String clientAccessToken;

  @Macro
  @Name(CLIENT_ACCESS_APPLICATION_NAME)
  @Nullable
  @Description("Search Console Client Application Name")
  protected String clientApplicationName;

  @Description("Path on the local file system of the service account key used "
    + "for authorization. "
    + "When running on other clusters, the file must be present on every node in the cluster.")
  @Macro
  @Name(SERVICE_ACCOUNT_FILE)
  @Nullable
  protected String serviceFilePath;

  @Description("Service Account JSON string")
  @Macro
  @Name(SERVICE_ACCOUNT_JSON)
  @Nullable
  protected String serviceAccountJson;

  @Macro
  @Name(SITES_FILTER_OPTION)
  @Description("List of site entries to query from. Can be set to All Site's - will retrieve "
    + "data from sites provide by Search Console Api.")
  private String sitesFilterOption;

  @Macro
  @Name(SITES_FILTERED_LIST)
  @Nullable
  @Description("List of sites to query from.")
  private String siteUrlList;

  @Name(NUM_SPLITS)
  @Macro
  @Description(
    "Desired number of splits to divide the sites into when reading from Search Console. "
      + "Fewer splits may be created if the sites cannot be divided into the desired number of splits.")
  private int numSplits;


  @Macro
  @Name(START_DATE)
  @Description("Start date in \u200BYYYY-MM-DD\n" +
    "format, in PT time (UTC -\n" +
    "7:00/8:00)")
  private String startDate;

  @Macro
  @Name(END_DATE)
  @Description("End date in \u200BYYYY-MM-DD\n" +
    "format, in PT time (UTC -\n" +
    "7:00/8:00)")
  private String endDate;

  @Macro
  @Name(DIMENSIONS)
  @Description("Comma separated list of dimensions to include")
  private String dimensions;

  @Name("schema")
  @Macro
  @Description("Specifies the schema of the records outputted from this plugin.")
  private String schema;

  public SearchConsoleSourceConfig(String referenceName, String sitesFilterOption,
                                   @Nullable String siteUrlList, String startDate, String endDate, String dimensions,
                                   String schema,
                                   @Nullable String clientId, @Nullable String clientSecret,
                                   @Nullable String clientAccessToken, @Nullable String clientApplicationName,
                                   @Nullable String serviceFilePath, @Nullable String serviceAccountJson,
                                   Integer numSplits, @Nullable String authenticationMethod) {
    super(referenceName);
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.clientAccessToken = clientAccessToken;
    this.clientApplicationName = clientApplicationName;
    this.serviceFilePath = serviceFilePath;
    this.serviceAccountJson = serviceAccountJson;
    this.sitesFilterOption = sitesFilterOption;
    this.siteUrlList = siteUrlList;
    this.numSplits = numSplits;
    this.startDate = startDate;
    this.endDate = endDate;
    this.dimensions = dimensions;
    this.schema = schema;
    this.authenticationMethod =
      (authenticationMethod == null) ? AUTHENTICATION_METHOD_OAUTH_CLIENT_ID
        : authenticationMethod;
  }

  private SearchConsoleSourceConfig(String referenceName) {
    super(referenceName);
  }

  static SearchConsoleSourceConfig of(String referenceName) {
    return new SearchConsoleSourceConfig(referenceName);
  }

  @Nullable
  public String getClientId() {
    return clientId;
  }

  @Nullable
  public String getClientSecret() {
    return clientSecret;
  }

  @Nullable
  public String getClientAccessToken() {
    return clientAccessToken;
  }

  @Nullable
  public String getClientApplicationName() {
    return clientApplicationName;
  }

  public String getStartDate() {
    return startDate;
  }

  public String getEndDate() {
    return endDate;
  }

  public void dateIsParsable(String date) throws ParseException {
    if (Strings.isNullOrEmpty(date)) {
      throw new ParseException("Date cannot be null.", 0);
    }
    DateFormat format = new SimpleDateFormat(ACCEPTED_DATE_FORMAT);
    format.parse(date);
  }

  public List<String> getDimensions() {
    return Arrays.stream(dimensions.trim().split(SearchConsoleConstants.DIMENSIONS_DELIMITER))
      .map(String::trim)
      .collect(Collectors.toList());
  }

  public String getSchema() {
    return schema;
  }

  public Schema getParsedSchema() throws IOException {
    return Schema.parseJson(getSchema());
  }

  public int getNumSplits() {
    return numSplits;
  }

  public void setNumSplits(int numSplits) {
    this.numSplits = numSplits;
  }

  public String getAuthenticationMethod() {
    return authenticationMethod;
  }

  public String getServiceAccountFilePath() {
    if (containsMacro(SERVICE_ACCOUNT_FILE) || Strings.isNullOrEmpty(serviceFilePath)) {
      return null;
    }
    return serviceFilePath;
  }

  @Nullable
  public String getServiceAccountJson() {
    if (containsMacro(SERVICE_ACCOUNT_JSON) || Strings.isNullOrEmpty(serviceAccountJson)) {
      return null;
    }
    return serviceAccountJson;
  }

  public Set<SiteInfo> getSiteEntries() {
    return Arrays.stream(getSiteUrlList().split(SearchConsoleConstants.SITE_DELIMITER))
      .map(siteInfo -> new SiteInfo(siteInfo.split(SearchConsoleConstants.SITE_KV_DELIMITER)[0],
                                    siteInfo.split(SearchConsoleConstants.SITE_KV_DELIMITER)[1]))
      .collect(Collectors.toSet());
  }

  public void setAuthenticationMethod(final String authenticationMethod) {
    this.authenticationMethod = authenticationMethod;
  }

  public void setClientId(@Nullable final String clientId) {
    this.clientId = clientId;
  }

  public void setClientSecret(@Nullable final String clientSecret) {
    this.clientSecret = clientSecret;
  }

  public void setClientAccessToken(@Nullable final String clientAccessToken) {
    this.clientAccessToken = clientAccessToken;
  }

  public void setClientApplicationName(@Nullable final String clientApplicationName) {
    this.clientApplicationName = clientApplicationName;
  }

  public void setServiceFilePath(@Nullable final String serviceFilePath) {
    this.serviceFilePath = serviceFilePath;
  }

  public void setServiceAccountJson(@Nullable final String serviceAccountJson) {
    this.serviceAccountJson = serviceAccountJson;
  }

  public void setSitesFilterOption(String sitesFilterOption) {
    this.sitesFilterOption = sitesFilterOption;
  }

  public String getSitesFilterOption() {
    return sitesFilterOption;
  }

  public void setSitesUrlList(String siteUrlList) {
    this.siteUrlList = siteUrlList;
  }

  public String getSiteUrlList() {
    return siteUrlList;
  }

  public void setStartDate(final String startDate) {
    this.startDate = startDate;
  }

  public void setEndDate(final String endDate) {
    this.endDate = endDate;
  }

  public void setDimensions(final String dimensions) {
    this.dimensions = dimensions;
  }

  public void setSchema(final String schema) {
    this.schema = schema;
  }

  public void validate(FailureCollector failureCollector) {
    if (!containsMacro(AUTHENTICATION_METHOD) && Strings
      .isNullOrEmpty(this.getAuthenticationMethod())) {
      failureCollector
        .addFailure("Missing authentication method.", "Authentication method must be set.");
      return;
    }
    if (!containsMacro(AUTHENTICATION_METHOD) && !Strings.isNullOrEmpty(this.getAuthenticationMethod())) {
      if (this.getAuthenticationMethod().equals("oAuthClient")) {
        if (!containsMacro(CLIENT_ID) && Strings.isNullOrEmpty(this.getClientId())) {
          failureCollector.addFailure("Missing client Id.", "Client Id must be set.");
        }
        if (!containsMacro(CLIENT_SECRET) && Strings.isNullOrEmpty(this.getClientSecret())) {
          failureCollector.addFailure("Missing client secret.", "Client secret must be set.");
        }
        if (!containsMacro(CLIENT_ACCESS_TOKEN) && Strings.isNullOrEmpty(this.getClientAccessToken())) {
          failureCollector
            .addFailure("Missing client access token.", "Client access token must be set.");
        }
      }
      if (this.getAuthenticationMethod().equals("serviceAccount")) {
        if (!containsMacro(SERVICE_ACCOUNT_FILE) && Strings.isNullOrEmpty(this.getServiceAccountFilePath())) {
          failureCollector
            .addFailure("Missing service account.", "Please provide service account file path.");
        }
      }
      if (this.getAuthenticationMethod().equals("serviceAccountJSON")) {
        if (!containsMacro(SERVICE_ACCOUNT_JSON) && Strings.isNullOrEmpty(this.getServiceAccountJson())) {
          failureCollector.addFailure("Missing service account json string.",
                                      "Please provide full json account string.");
        }
      }
    }

    if (!containsMacro(START_DATE)) {
      try {
        dateIsParsable(this.getStartDate());
      } catch (ParseException e) {
        failureCollector
          .addFailure("Missing/Invalid Start Date.", "Please provide valid Start Date.");
        return;
      }
    }
    if (!containsMacro(END_DATE)) {
      try {
        dateIsParsable(this.getEndDate());
      } catch (ParseException e) {
        failureCollector.addFailure("Missing/Invalid End Date.", "Please provide valid End Date.");
        return;
      }
    }
    if (!containsMacro(AUTHENTICATION_METHOD) && !containsMacro(CLIENT_ID) && !containsMacro(CLIENT_SECRET)
      && !containsMacro(CLIENT_ACCESS_TOKEN) && !containsMacro(SERVICE_ACCOUNT_FILE)
      && !containsMacro(SERVICE_ACCOUNT_JSON)) {
      try {
        // check if custom site list is not empty when the option is selected
        if (!containsMacro(SITES_FILTER_OPTION) && !containsMacro(SITES_FILTERED_LIST) &&
          this.getSitesFilterOption().equals(SearchConsoleConstants.SITES_FILTER_OPTION_LIST)
          && !Optional.ofNullable(getSiteUrlList()).isPresent()) {
          failureCollector.addFailure("Site list empty.", "Site list needs to provided.");
          return;
        }
        Webmasters service = SearchConsoleUtils.generateService(this);
        List<String> siteList = SearchConsoleUtils.queryAllSites(service);
        if (siteList.size() == 0) {
          failureCollector.addFailure("Error:", "No sites retrieved from API.");
          return;
        }
        if (!containsMacro(SITES_FILTER_OPTION) && this.getSitesFilterOption()
          .equals(SearchConsoleConstants.SITES_FILTER_OPTION_LIST)) {
          if (!Optional.ofNullable(SearchConsoleUtils.getSitesFormList(this)).isPresent()) {
            failureCollector.addFailure("Site not on the list.",
                                        "Please make sure provided account has access to sites in provided list.");
            return;
          } else {
            checkSitesForAccessibility(siteList, failureCollector);
          }
        }
      } catch (IOException e) {
        failureCollector.addFailure(e.getMessage(), e.getMessage());
      }
    }
    failureCollector.getOrThrowException();
  }

  /**
   * Itereate through each site on the list and check if it's accessible and log as failure if site
   * is not accessible
   *
   * @param siteList         {@link List<String>}
   * @param failureCollector {@link FailureCollector}
   */
  private void checkSitesForAccessibility(List<String> siteList, FailureCollector failureCollector) {
    for (SiteInfo site : this.getSiteEntries()) {
      if (!siteList.contains(site.getSiteUrlWithType())) {
        failureCollector.addFailure("Site not accessible:", site.getSiteUrl());
      }
    }
  }

  public static SearchConsoleSourceConfig of(JsonObject properties) {
    SearchConsoleSourceConfig searchConsoleSourceConfig = SearchConsoleSourceConfig
      .of(properties.get(SearchConsoleSourceConfig.PROPERTY_REFERENCE_NAME).getAsString());

    if (properties.has(SearchConsoleSourceConfig.AUTHENTICATION_METHOD)) {
      searchConsoleSourceConfig.setAuthenticationMethod(
        properties.get(SearchConsoleSourceConfig.AUTHENTICATION_METHOD).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.CLIENT_ID)) {
      searchConsoleSourceConfig
        .setClientId(properties.get(SearchConsoleSourceConfig.CLIENT_ID).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.CLIENT_SECRET)) {
      searchConsoleSourceConfig
        .setClientSecret(properties.get(SearchConsoleSourceConfig.CLIENT_SECRET).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.CLIENT_ACCESS_TOKEN)) {
      searchConsoleSourceConfig.setClientAccessToken(
        properties.get(SearchConsoleSourceConfig.CLIENT_ACCESS_TOKEN).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.SERVICE_ACCOUNT_JSON)) {
      searchConsoleSourceConfig.setServiceAccountJson(
        properties.get(SearchConsoleSourceConfig.SERVICE_ACCOUNT_JSON).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.SERVICE_ACCOUNT_FILE)) {
      searchConsoleSourceConfig.setServiceFilePath(
        properties.get(SearchConsoleSourceConfig.SERVICE_ACCOUNT_FILE).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.SITES_FILTER_OPTION)) {
      searchConsoleSourceConfig.setSitesFilterOption(
        properties.get(SearchConsoleSourceConfig.SITES_FILTER_OPTION).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.SITES_FILTERED_LIST)) {
      searchConsoleSourceConfig.setSitesUrlList(
        properties.get(SearchConsoleSourceConfig.SITES_FILTERED_LIST).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.DIMENSIONS)) {
      searchConsoleSourceConfig
        .setDimensions(properties.get(SearchConsoleSourceConfig.DIMENSIONS).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.START_DATE)) {
      searchConsoleSourceConfig
        .setStartDate(properties.get(SearchConsoleSourceConfig.START_DATE).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.END_DATE)) {
      searchConsoleSourceConfig
        .setEndDate(properties.get(SearchConsoleSourceConfig.END_DATE).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.SCHEMA)) {
      searchConsoleSourceConfig
        .setSchema(properties.get(SearchConsoleSourceConfig.SCHEMA).getAsString());
    }

    if (properties.has(SearchConsoleSourceConfig.NUM_SPLITS)) {
      searchConsoleSourceConfig
        .setNumSplits(properties.get(SearchConsoleSourceConfig.NUM_SPLITS).getAsInt());
    }

    return searchConsoleSourceConfig;

  }

}
