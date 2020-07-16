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
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.webmasters.Webmasters;
import com.google.api.services.webmasters.model.WmxSite;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Common utils for search console api
 */
public final class SearchConsoleUtils {

  private static final NetHttpTransport httpTransport = new NetHttpTransport();
  private static final JsonFactory jsonFactory = new JacksonFactory();
  public static final Gson GSON = new GsonBuilder().create();

  private SearchConsoleUtils() {
  }

  public static SearchConsoleSourceConfig extractPropertiesFromConfig(Configuration configuration) {
    String configJson = configuration.get(SearchConsoleConstants.CONFIGURATION_PROPERTY_NAME);
    JsonObject properties = GSON.fromJson(configJson, JsonObject.class)
      .getAsJsonObject(SearchConsoleConstants.CONFIGURATION_PARSE_PROPERTY_NAME);
    return SearchConsoleSourceConfig.of(properties);
  }

  /**
   * Generates list of site Urls based on config If configuration is set to auto it will trigger API
   * call to get the list of sites/domains from Search Console API otherwise it will read from
   * plugin configuration
   *
   * @param consoleSourceConfig {@link SearchConsoleSourceConfig}
   * @return string list with sites - prefixes included
   * @throws IOException throws exceptions if list cannot be retrieved from API
   */
  public static List<String> getSitesUrls(SearchConsoleSourceConfig consoleSourceConfig)
    throws IOException {
    String sitesFilterOption = consoleSourceConfig.getSitesFilterOption();
    switch (sitesFilterOption) {
      case SearchConsoleConstants.SITES_FILTER_OPTION_ALL_URLS:
        return getAllSiteEntries(consoleSourceConfig);
      case SearchConsoleConstants.SITES_FILTER_OPTION_LIST:
        return getSitesFormList(consoleSourceConfig);
      default:
        return new ArrayList<>();
    }
  }

  /**
   * Reads list from sites from Search Console API and returns as string list
   *
   * @param consoleSourceConfig {@link SearchConsoleSourceConfig}
   * @return {@link List<String>}
   * @throws IOException when get site list query fails
   */
  private static List<String> getAllSiteEntries(SearchConsoleSourceConfig consoleSourceConfig)
    throws IOException {
    GoogleCredential googleCredential = generateCredential(consoleSourceConfig);
    Webmasters webmasters = generateService(googleCredential);
    return queryAllSites(webmasters);
  }

  /**
   * Reads list from sites from Search Console API service and returns as string list
   *
   * @param service {@link Webmasters}
   * @return {@link List<String>}
   * @throws IOException when get site list query fails
   */
  public static List<String> queryAllSites(Webmasters service) throws IOException {
    Webmasters.Sites.List siteListRequest = service.sites().list();
    final List<WmxSite> availableSiteEntries = siteListRequest.execute().getSiteEntry();
    return availableSiteEntries.stream().map(WmxSite::getSiteUrl)
      .collect(Collectors.toList());
  }

  /**
   * Reads list from sites from Config property and returns as set of {@link SiteInfo}
   *
   * @param consoleSourceConfig {@link SearchConsoleSourceConfig}
   * @return {@link Set<SiteInfo>}
   */
  public static Set<SiteInfo> getSiteEntriesFromList(
    SearchConsoleSourceConfig consoleSourceConfig) {
    if (Optional.ofNullable(consoleSourceConfig.getSiteUrlList()).isPresent()) {
      return Arrays
        .stream(consoleSourceConfig.getSiteUrlList().split(SearchConsoleConstants.SITE_DELIMITER))
        .map(siteInfo -> new SiteInfo(siteInfo.split(SearchConsoleConstants.SITE_KV_DELIMITER)[0],
                                      siteInfo.split(SearchConsoleConstants.SITE_KV_DELIMITER)[1]))
        .collect(Collectors.toSet());
    }
    return new HashSet<>();
  }

  /**
   * Reads list from sites from Config property and returns as string list
   *
   * @param consoleSourceConfig {@link SearchConsoleSourceConfig}
   * @return {@link List<String>}
   */
  public static List<String> getSitesFormList(SearchConsoleSourceConfig consoleSourceConfig) {
    return getSiteEntriesFromList(consoleSourceConfig).stream()
      .map(SiteInfo::getSiteUrlWithType).collect(Collectors.toList());
  }

  /**
   * Generates Google Credentials based on provided configuration Currently supported methods 'OAuth
   * Client', 'Service Account File'
   *
   * @return GoogleCredential initialized with provided config
   * @throws IOException on wrong credentials or network issue
   */
  public static GoogleCredential generateCredential(SearchConsoleSourceConfig consoleSourceConfig)
    throws IOException {
    GoogleCredential credential = null;
    GoogleCredential.Builder builder = new GoogleCredential.Builder();
    builder.setTransport(httpTransport);
    builder.setJsonFactory(jsonFactory);
    switch (consoleSourceConfig.getAuthenticationMethod()) {
      case SearchConsoleConstants.AUTH_TYPE_OAUTH:
        if (!Optional.ofNullable(consoleSourceConfig.getClientId()).isPresent()) {
          throw new IllegalArgumentException("Client ID is not set");
        }
        builder.setClientSecrets(consoleSourceConfig.getClientId(), consoleSourceConfig.getClientSecret());
        credential = builder.build();
        credential.setAccessToken(consoleSourceConfig.getClientAccessToken());
        break;
      case SearchConsoleConstants.AUTH_TYPE_SERVICE_ACCOUNT_FILE:
        if (SearchConsoleSourceConfig.AUTO_DETECT.equals(consoleSourceConfig.getServiceAccountFilePath())) {
          credential = GoogleCredential.getApplicationDefault();
        } else {
          File credentialPath = new File(consoleSourceConfig.getServiceAccountFilePath());
          InputStream inputStream = new FileInputStream(credentialPath);
          credential = GoogleCredential.fromStream(inputStream, httpTransport, jsonFactory)
            .createScoped(Collections.singletonList(SearchConsoleSourceConfig.OAUTH_SCOPE));
        }
        break;
      case SearchConsoleConstants.AUTH_TYPE_SERVICE_ACCOUNT_JSON:
        if (!Optional.ofNullable(consoleSourceConfig.getServiceAccountJson()).isPresent()) {
          throw new IllegalArgumentException("Service Account Json content can not be empty");
        }
        InputStream jsonInputStream = new ByteArrayInputStream(
          consoleSourceConfig.getServiceAccountJson().getBytes());
        credential = GoogleCredential.fromStream(jsonInputStream, httpTransport, jsonFactory)
          .createScoped(Collections.singletonList(SearchConsoleSourceConfig.OAUTH_SCOPE));
        break;
    }
    return credential;
  }

  /**
   * Generate Webmasters service based on provided configuration
   *
   * @return Webmasters service
   */
  public static Webmasters generateService(GoogleCredential credential) {
    return new Webmasters.Builder(httpTransport, jsonFactory, credential).build();
  }

  /**
   * Generates {@link Webmasters} service from {@link SearchConsoleSourceConfig}
   *
   * @param config {@link SearchConsoleSourceConfig}
   * @return {@link Webmasters}
   * @throws IOException when fails to generate service
   */
  public static Webmasters generateService(SearchConsoleSourceConfig config) throws IOException {
    return SearchConsoleUtils.generateService(SearchConsoleUtils.generateCredential(config));
  }

  /**
   * Converts field into corresponding type based on logical type provided by field Schema
   *
   * @param field       Any type of {@link Object}
   * @param logicalType {@link Schema.LogicalType} to convert to
   * @param fieldSchema {@link Schema} schema expected to be matched by field
   * @return Ant type of
   */
  private static Object convertLogicalByType(Object field, Schema.LogicalType logicalType,
                                             Schema fieldSchema) {
    switch (logicalType) {
      case DATE:
        // date will be in yyyy-mm-dd format
        return Math.toIntExact(LocalDate.parse(field.toString()).toEpochDay());
      case TIME_MILLIS:
        // time will be in hh:mm:ss format
        return Math.toIntExact(TimeUnit.NANOSECONDS.toMillis(LocalTime.parse(field.toString()).toNanoOfDay()));
      case TIME_MICROS:
        // time will be in hh:mm:ss format
        return TimeUnit.NANOSECONDS.toMicros(LocalTime.parse(field.toString()).toNanoOfDay());
      case TIMESTAMP_MILLIS:
        // by default set to start of day as api only returns date part
        return Math.toIntExact(LocalDate.parse(field.toString()).atStartOfDay().toInstant(ZoneOffset.UTC)
                                 .toEpochMilli());
      case TIMESTAMP_MICROS:
        // by default set to start of day as api only returns date part
        return LocalDate.parse(field.toString()).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
      case DECIMAL:
        ByteBuffer value = (ByteBuffer) field;
        byte[] bytes = new byte[value.remaining()];
        int pos = value.position();
        value.get(bytes);
        value.position(pos);
        return bytes;
      default:
        throw new UnexpectedFormatException(
          "Field type '" + fieldSchema.getDisplayName() + "' is not supported.");
    }
  }

  /**
   * Converts field into corresponding type based on type provided by field Schema
   *
   * @param field     Any type of {@link Object}
   * @param fieldType {@link Schema.Type} to convert to
   * @return {@link Schema} schema expected to be matched by field
   */
  private static Object convertFieldByType(Object field, Schema.Type fieldType) {
    switch (fieldType) {
      case INT:
        return Integer.valueOf(field.toString());
      case DOUBLE:
        return Double.valueOf(field.toString());
      case FLOAT:
        return Float.valueOf(field.toString());
      case BOOLEAN:
        return Boolean.valueOf(field.toString());
      case LONG:
        return Long.valueOf(field.toString());
      case STRING:
        return field.toString();
      default:
        return field;
    }
  }

  /**
   * Converts object type to match provided schema type
   *
   * @param field       Any type of {@link Object}
   * @param fieldSchema {@link Schema} to convert to
   * @return Any type of {@link Object}
   * @throws IOException when fails to convert the object
   */
  public static Object convertField(Object field, Schema fieldSchema) throws IOException {
    if (field == null) {
      return null;
    }

    fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
    Schema.Type fieldType = fieldSchema.getType();
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();

    try {
      if (logicalType != null) {
        return convertLogicalByType(field, logicalType, fieldSchema);
      } else {
        return convertFieldByType(field, fieldType);
      }
    } catch (ArithmeticException e) {
      throw new IOException("Field type %s has value that is too large." + fieldType);
    }
  }

}
