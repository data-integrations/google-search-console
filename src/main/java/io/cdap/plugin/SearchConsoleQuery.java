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

import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.common.Bytes;
import java.util.List;

/**
 * Class for storing list of sites to be utilized by split
 */
public class SearchConsoleQuery {

  private List<String> sites;

  public SearchConsoleQuery(List<String> sites) {
    this.sites = sites;
  }

  public List<String> getSites() {
    return sites;
  }

  public byte[] toByteArray() {
    return new GsonBuilder().create().toJson(this).getBytes();
  }

  static SearchConsoleQuery parseFrom(byte[] bytes) {
    return new GsonBuilder().create().fromJson(Bytes.toString(bytes), SearchConsoleQuery.class);
  }

}
