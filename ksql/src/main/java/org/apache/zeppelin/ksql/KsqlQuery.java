/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.ksql;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KsqlQuery {
  private static final Logger LOGGER = LoggerFactory.getLogger(KsqlQuery.class);

  public enum QueryType {
    SHOW_TABLES,
    SHOW_STREAMS,
    DESCRIBE,
    EXPLAIN,
    SHOW_PROPS,
    SELECT,
    UNSUPPORTED
  }

  QueryType type;
  String query;
  List<String> captures = Collections.emptyList();

  private static List<Pair<Pattern, QueryType>> PATTERNS;

  static {
    PATTERNS = new ArrayList<>();
    PATTERNS.add(Pair.of(Pattern.compile("^(?:show|list)\\s+streams\\s*;\\s*$",
        Pattern.CASE_INSENSITIVE), QueryType.SHOW_STREAMS));
    PATTERNS.add(Pair.of(Pattern.compile("^(?:show|list)\\s+tables\\s*;\\s*$",
        Pattern.CASE_INSENSITIVE), QueryType.SHOW_TABLES));
    PATTERNS.add(Pair.of(Pattern.compile("^(?:show|list)\\s+properties\\s*;\\s*$",
        Pattern.CASE_INSENSITIVE), QueryType.SHOW_PROPS));
  }

  KsqlQuery() {
    type = QueryType.UNSUPPORTED;
  }

  KsqlQuery(final String q) {
    query = q;
    type = analyzeQuery(q);
    LOGGER.debug("Initializing KsqlQuery: {}, type: {}", query, type.name());
  }

  public boolean isUnsupported() {
    return type == QueryType.UNSUPPORTED;
  }

  public QueryType getType() {
    return type;
  }

  public void setType(QueryType type) {
    this.type = type;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public QueryType analyzeQuery(final String query) {
    for (Pair<Pattern, QueryType> pair : PATTERNS) {
      Matcher m = pair.getLeft().matcher(query);
      if (m.matches()) {
        int groupCount = m.groupCount();
        if (groupCount > 0) {
          captures = new ArrayList<>(groupCount);
          for (int i = 1; i < groupCount + 1; i++) {
            captures.add(m.group(i));
          }
        }
        return pair.getRight();
      }
    }
    return QueryType.UNSUPPORTED;
  }
}
