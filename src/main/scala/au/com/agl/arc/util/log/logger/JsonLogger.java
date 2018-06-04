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

public interface JsonLogger {

  /**
   * Set top level message field.  Convenience method for .field("message", ... )
   */
  JsonLogger message(String message);

  /**
   * Set top level message field as a lambda or supplier that is lazily evaluated only if the message is logged
   */
  JsonLogger message(Supplier<String> message);

  /**
   * Add a map to the JSON hierarchy
   */
  JsonLogger map(String key, Map map);

  /**
   * Add a map to the JSON hierarchy as a lambda or supplier that is lazily evaluated only if the message is logged
   */
  JsonLogger map(String key, Supplier<Map> map);

  /**
   * Add a list to the JSON hierarchy
   */
  JsonLogger list(String key, List list);

  /**
   * Add a list to the JSON hierarchy as a lambda or supplier that is lazily evaluated only if the message is logged
   */
  JsonLogger list(String key, Supplier<List> list);

  /**
   * Add a top level field.
   * null values will be represented as the json null primitive
   */
  JsonLogger field(String key, Object value);

  /**
   * Add a top level field as a lambda or supplier that is lazily evaluated only if the message is logged.
   * null values will be represented as the json null primitive
   */
  JsonLogger field(String key, Supplier value);

  /**
   * Add an arbitrary JsonElement object to the top level with the given key.
   */
  JsonLogger json(String key, JsonElement jsonElement);

  /**
   * Add an arbitrary JsonElement object to the top level with the given key that is lazily evaluated only if the message is logged
   */
  JsonLogger json(String key, Supplier<JsonElement> jsonElement);

  /**
   * Add an exception to the JSON hierarchy.  The exception will be formatted to include the message and the stacktrace
   * similar to how it is outputted using exception.printStackTrace()
   */
  JsonLogger exception(String key, Exception exception);

  /**
   * Include the stack dump of the current running thread in the log output.
   * This data will be included in the output under the "stacktrace" key
   */
  JsonLogger stack();

  /**
   * Set the marker to use when generating the log message.
   *
   * @param marker marker to use when generating the log message.  See details in the SLF4J Logger
   *               API documents.
   */
  JsonLogger marker(Marker marker);

  /**
   * Log the formatted message
   */
  void log();
}
