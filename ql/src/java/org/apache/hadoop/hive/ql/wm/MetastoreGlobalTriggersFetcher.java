/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.wm;

import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Hive;

/**
 * Fetch global (non-llap) rules from metastore
 */
public class MetastoreGlobalTriggersFetcher implements TriggersFetcher {
  public static final String GLOBAL_TRIGGER_NAME = "global";
  private final MetastoreResourcePlanTriggersFetcher rpTriggersFetcher;

  public MetastoreGlobalTriggersFetcher(final Hive db) {
    this.rpTriggersFetcher = new MetastoreResourcePlanTriggersFetcher(db);
  }

  @Override
  public List<Trigger> fetch(final String ignore) {
    return fetch();
  }

  public List<Trigger> fetch() {
    // TODO:
    return rpTriggersFetcher.fetch(GLOBAL_TRIGGER_NAME);
  }
}
