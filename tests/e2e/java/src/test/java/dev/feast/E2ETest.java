/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.feast;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.gojek.feast.FeastClient;
import com.gojek.feast.Row;
import feast.common.auth.credentials.GoogleAuthCredentials;
import feast.proto.types.ValueProto.Value;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * E2E tests Java SDK retrieval with Feast. Expects Feast to have a registered Feature Set
 * 'customer_transactions' that is already ingested with data and ready for retrieval. See
 * tests/e2e/python/redis/basic-ingest-redis-serving.py for test setup code. Properties used to
 * configure test params: feast.serving.host - hostname of the feast serving instance to connect to.
 * feast.serving.port - port of the feast serving instance to connect to. feast.auth.enabled - if
 * true, uses authentication to connect to Feast serving.
 */
public class E2ETest {
  private static FeastClient client;
  private static boolean isAuthenticationEnabled;

  @BeforeAll
  public static void setup() throws IOException {
    // Read test params from properties.
    Properties properties = System.getProperties();
    String servingHost = properties.getProperty("feast.serving.host");
    int servingPort = Integer.parseInt(properties.getProperty("feast.serving.port"));
    isAuthenticationEnabled = Boolean.parseBoolean(properties.getProperty("feast.auth.enabled"));

    // Setup clients for test.
    if (!isAuthenticationEnabled) {
      client = FeastClient.create(servingHost, servingPort);
    } else {
      // Uses GoogleAuthCredentials to authenticate
      // Expects Google Application Credentials to configured with Service Account JSON.
      String audience = String.format("%s:%d", servingHost, servingPort);
      client =
          FeastClient.createAuthenticated(
              servingHost, servingPort, new GoogleAuthCredentials(Map.of("audience", audience)));
    }
  }

  @Test
  public void shouldGetOnlineFeatures() {
    List<String> featureRefs =
        List.of("daily_transactions", "customer_transactions:total_transactions");

    List<Row> featureRows =
        client.getOnlineFeatures(
            featureRefs,
            List.of(Row.create().set("customer_id", Value.newBuilder().setInt64Val(1).build())));
    assertThat(featureRows.size(), equalTo(1));

    // Check feature values and statuses retrieved are correct.
    // Values are ingested in tests/e2e/python/redis/basic-ingest-redis-serving.py.
    Map<String, Value> fields = featureRows.get(0).getFields();
    System.out.println(fields.keySet());
    featureRefs.stream()
        .map(ref -> fields.get(ref))
        .forEach(value -> assertEquals(value.getFloatVal(), 1.0f, 0.00001f));
  }
}
