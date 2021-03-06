// Copyright 2018 Lawrence Livermore National Security, LLC and other
// nfs-directory-source Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

/**
 * This file has been modified from its originally licensed form.
 *
 * This software is made available by Confluent, Inc., under the
 * terms of the Confluent Community License Agreement, Version 1.0
 * located at http://www.confluent.io/confluent-community-license.  BY
 * INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY OF
 * THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

import java.util.HashMap;
import java.util.Map;

public class AvroConverterConfig extends AbstractKafkaAvroSerDeConfig {

  public AvroConverterConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  public static class Builder {

    private Map<String, Object> props = new HashMap<>();

    public Builder with(String key, Object value) {
      props.put(key, value);
      return this;
    }

    public AvroConverterConfig build() {
      return new AvroConverterConfig(props);
    }
  }
}
