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

package ai.tripl.arc.util.log.logger;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.MDC;
import org.slf4j.Marker;

import java.text.Format;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class StandardJsonLogger implements JsonLogger {
  private final org.slf4j.Logger slf4jLogger;
  private final FastDateFormat formatter;
  private final Gson gson;
  private final JsonObject jsonObject;

  // LEVEL-Specific Settings
  private final Consumer<String> logOperation;
  private final BiConsumer<Marker, String> logWithMarkerOperation;
  private final String levelName;

  private Marker marker;

  private boolean includeLoggerName = true;
  private boolean includeThreadName = true;
  private boolean includeClassName = true;

  public StandardJsonLogger(org.slf4j.Logger slf4jLogger, FastDateFormat formatter, Gson gson, String levelName,
      Consumer<String> logOperation, BiConsumer<Marker, String> logWithMarkerOperation) {
    this.slf4jLogger = slf4jLogger;
    this.formatter = formatter;
    this.gson = gson;

    this.levelName = levelName;
    this.logOperation = logOperation;
    this.logWithMarkerOperation = logWithMarkerOperation;

    this.jsonObject = new JsonObject();
  }

  // ========================================
  // Getters and Setters
  // ----------------------------------------

  public boolean isIncludeLoggerName() {
    return includeLoggerName;
  }

  public void setIncludeLoggerName(boolean includeLoggerName) {
    this.includeLoggerName = includeLoggerName;
  }

  public boolean isIncludeThreadName() {
    return includeThreadName;
  }

  public void setIncludeThreadName(boolean includeThreadName) {
    this.includeThreadName = includeThreadName;
  }

  public boolean isIncludeClassName() {
    return includeClassName;
  }

  public void setIncludeClassName(boolean includeClassName) {
    this.includeClassName = includeClassName;
  }

  // ========================================
  // Public API
  // ----------------------------------------

  @Override
  public JsonLogger message(String message) {
    try {
      jsonObject.add("message", gson.toJsonTree(message));
    } catch (Exception e) {
      jsonObject.add("message", gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger message(Supplier<String> message) {
    try {
      jsonObject.add("message", gson.toJsonTree(message.get()));
    } catch (Exception e) {
      jsonObject.add("message", gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger map(String key, Map map) {
    try {
      jsonObject.add(key, gson.toJsonTree(map));
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger map(String key, Supplier<Map> map) {
    try {
      jsonObject.add(key, gson.toJsonTree(map.get()));
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger list(String key, List list) {
    try {
      jsonObject.add(key, gson.toJsonTree(list));
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger list(String key, Supplier<List> list) {
    try {
      jsonObject.add(key, gson.toJsonTree(list.get()));
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger field(String key, Object value) {
    try {
      jsonObject.add(key, gson.toJsonTree(value));
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger field(String key, Supplier value) {
    try {
      // in the rare case that the value passed is null, this method will be selected
      // as more specific than the Object
      // method. Have to handle it here or the value.get() will NullPointer
      if (value == null) {
        jsonObject.add(key, null);
      } else {
        jsonObject.add(key, gson.toJsonTree(value.get()));
      }
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger json(String key, JsonElement jsonElement) {
    try {
      jsonObject.add(key, jsonElement);
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger json(String key, Supplier<JsonElement> jsonElement) {
    try {
      jsonObject.add(key, jsonElement.get());
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger exception(String key, Exception exception) {
    try {
      jsonObject.add(key, gson.toJsonTree(formatException(exception)));
    } catch (Exception e) {
      jsonObject.add(key, gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger stack() {
    try {
      jsonObject.add("stacktrace", gson.toJsonTree(formatStack()));
    } catch (Exception e) {
      jsonObject.add("stacktrace", gson.toJsonTree(formatException(e)));
    }
    return this;
  }

  @Override
  public JsonLogger marker(Marker marker) {
    this.marker = marker;
    jsonObject.add("marker", gson.toJsonTree(marker.getName()));

    return this;
  }

  @Override
  public void log() {
    String message = this.formatMessage(levelName);

    if (this.marker == null) {
      this.logOperation.accept(message);
    } else {
      this.logWithMarkerOperation.accept(marker, message);
    }
  }

  // ========================================
  // Internals
  // ----------------------------------------

  protected String formatMessage(String level) {

    jsonObject.add("level", gson.toJsonTree(level));

    if (includeThreadName) {
      jsonObject.add("thread_name", gson.toJsonTree(Thread.currentThread().getName()));
    }

    if (includeClassName) {
      try {
        jsonObject.add("class", gson.toJsonTree(getCallingClass()));
      } catch (Exception e) {
        jsonObject.add("class", gson.toJsonTree(formatException(e)));
      }
    }

    if (includeLoggerName) {
      jsonObject.add("logger_name", gson.toJsonTree(slf4jLogger.getName()));
    }

    try {
      jsonObject.add("timestamp", gson.toJsonTree(getCurrentTimestamp(formatter)));
    } catch (Exception e) {
      jsonObject.add("timestamp", gson.toJsonTree(formatException(e)));
    }

    Map mdc = MDC.getCopyOfContextMap();
    if (mdc != null && !mdc.isEmpty()) {
      try {
        mdc.forEach((k, v) -> jsonObject.add(k.toString(), gson.toJsonTree(v)));
      } catch (Exception e) {
        jsonObject.add("mdc", gson.toJsonTree(formatException(e)));
      }
    }

    return gson.toJson(jsonObject);
  }

  private String getCallingClass() {
    StackTraceElement[] stackTraceElements = (new Exception()).getStackTrace();
    return stackTraceElements[3].getClassName();
  }

  private String getCurrentTimestamp(Format formatter) {
    return formatter.format(System.currentTimeMillis());
  }

  private String formatException(Exception e) {
    return ExceptionUtils.getStackTrace(e);
  }

  /**
   * Some contention over performance of Thread.currentThread.getStackTrace() vs
   * (new Exception()).getStackTrace() Code in Thread.java actually uses the
   * latter if 'this' is the current thread so we do the same
   *
   * Remove the top two elements as those are the elements from this logging class
   */
  private String formatStack() {
    StringBuilder output = new StringBuilder();
    StackTraceElement[] stackTraceElements = (new Exception()).getStackTrace();
    output.append(stackTraceElements[2]);
    for (int index = 3; index < stackTraceElements.length; index++) {
      output.append("\n\tat ").append(stackTraceElements[index]);
    }
    return output.toString();
  }
}