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

import java.util.Objects;

/**
 * Class representing single site info
 */
public final class SiteInfo {

  private final String siteUrl;
  private final String siteType;

  public SiteInfo(String siteUrl, String siteType) {
    this.siteUrl = siteUrl;
    this.siteType = siteType;
  }

  public String getSiteUrl() {
    return siteUrl;
  }

  public String getSiteUrlWithType() {
    return String.format("%s%s", SearchConsoleConstants.SITE_TYPE_DOMAIN.equals(siteType)
      ? SearchConsoleConstants.SEARCH_CONSOLE_DOMAIN_PREFIX : "", siteUrl);
  }

  @Override
  public String toString() {
    return "SiteInfo{" +
      "siteUrl='" + siteUrl + '\'' +
      ", siteType='" + siteType + '\'' +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SiteInfo siteInfo = (SiteInfo) o;
    return Objects.equals(siteUrl, siteInfo.siteUrl) &&
      Objects.equals(siteType, siteInfo.siteType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(siteUrl, siteType);
  }
}
