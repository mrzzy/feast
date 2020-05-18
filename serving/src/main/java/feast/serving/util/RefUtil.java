/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2019 The Feast Authors
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
package feast.serving.util;

import feast.proto.core.FeatureSetProto.FeatureSetSpec;
import feast.proto.serving.ServingAPIProto.FeatureReference;

public class RefUtil {
  public static String generateFeatureStringRef(FeatureReference featureReference) {
    String ref = featureReference.getName();
    if (!featureReference.getFeatureSet().isEmpty()) {
      ref = featureReference.getFeatureSet() + ":" + ref;
    }
    if (!featureReference.getProject().isEmpty()) {
      ref = featureReference.getProject() + "/" + ref;
    }
    return ref;
  }

  public static String generateFeatureSetStringRef(FeatureSetSpec featureSetSpec) {
    String ref = String.format("%s/%s", featureSetSpec.getProject(), featureSetSpec.getName());
    return ref;
  }
}
