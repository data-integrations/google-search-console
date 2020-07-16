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
 * Constants for Google Search Console plugins
 */
public class SearchConsoleConstants {

  public static final String CONFIGURATION_PROPERTY_NAME = "search.console.config";
  public static final String CONFIGURATION_PARSE_PROPERTY_NAME = "properties";
  public static final String DIMENSIONS_PROPERTY_NAME = "keys";
  public static final String SEARCH_CONSOLE_DOMAIN_PREFIX = "sc-domain:";
  public static final int SEARCH_CONSOLE_MAX_ROW_LIMIT = 25000;
  public static final String SITE_TYPE_DOMAIN = "Domain";
  public static final String SITE_TYPE_URL_PREFIX = "Url Prefix";
  public static final String SITE_KV_DELIMITER = "__SITE_KV_DELIMITER__";
  public static final String SITE_DELIMITER = "__SITE_DELIMITER__";
  public static final String DIMENSIONS_DELIMITER = ",";
  public static final String AUTH_TYPE_OAUTH = "oAuthClient";
  public static final String AUTH_TYPE_SERVICE_ACCOUNT_FILE = "serviceAccount";
  public static final String AUTH_TYPE_SERVICE_ACCOUNT_JSON = "serviceAccountJson";
  public static final String SITES_FILTER_OPTION_ALL_URLS = "allUrlsOption";
  public static final String SITES_FILTER_OPTION_LIST = "siteUrlListOption";
}
