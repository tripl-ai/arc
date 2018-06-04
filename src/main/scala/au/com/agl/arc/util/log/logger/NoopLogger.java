/*
 * Copyright (c) 2016 Savoir Technologies
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
 *
 *
 */

package au.com.agl.arc.util.log.logger;

import com.google.gson.JsonElement;

import org.slf4j.Marker;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class NoopLogger implements JsonLogger {

  @Override
  public JsonLogger map(String key, Map map) {
    return this;
  }

  @Override
  public JsonLogger map(String key, Supplier<Map> map) {
    return this;
  }

  @Override
  public JsonLogger list(String key, List list) {
    return this;
  }

  @Override
  public JsonLogger list(String key, Supplier<List> list) {
    return this;
  }

  @Override
  public JsonLogger message(String message) {
    return this;
  }

  @Override
  public JsonLogger message(Supplier<String> message) {
    return this;
  }

  @Override
  public JsonLogger field(String key, Object value) {
    return this;
  }

  @Override
  public JsonLogger field(String key, Supplier value) {
    return this;
  }

  @Override
  public JsonLogger json(String key, JsonElement jsonElement) {
    return this;
  }

  @Override
  public JsonLogger json(String key, Supplier<JsonElement> jsonElement) {
    return this;
  }

  @Override
  public JsonLogger exception(String key, Exception exception) {
    return this;
  }

  @Override
  public JsonLogger stack() { return this; }

  @Override
  public JsonLogger marker(Marker marker) {
    return this;
  }

  @Override
  public void log() {

  }
}
