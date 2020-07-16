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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Split for search console
 */
public class SearchConsoleSplit extends InputSplit implements Writable {

  private SearchConsoleQuery query;

  public SearchConsoleSplit() {
    // is needed for Hadoop deserialization
  }

  public SearchConsoleSplit(SearchConsoleQuery query) {
    this.query = query;
  }

  @Override
  public void write(final DataOutput dataOutput) throws IOException {
    byte[] bytes = query.toByteArray();
    dataOutput.writeInt(bytes.length);
    dataOutput.write(bytes);
  }

  @Override
  public void readFields(final DataInput dataInput) throws IOException {
    int length = dataInput.readInt();
    byte[] bytes = new byte[length];
    dataInput.readFully(bytes);
    query = SearchConsoleQuery.parseFrom(bytes);
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public String[] getLocations() {
    return new String[0];
  }

  public SearchConsoleQuery getQuery() {
    return this.query;
  }
}
