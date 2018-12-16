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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.commons.lang3.time.FastDateFormat;

/**
 * Wrapper for slf4j Logger that enables a builder pattern and JSON layout
 */
public class Logger {
  public static final String TRACE_LEVEL_NAME = "TRACE";
  public static final String DEBUG_LEVEL_NAME = "DEBUG";
  public static final String INFO_LEVEL_NAME = "INFO";
  public static final String WARN_LEVEL_NAME = "WARN";
  public static final String ERROR_LEVEL_NAME = "ERROR";

  private org.slf4j.Logger slf4jLogger;

  private Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();
  private FastDateFormat formatter;
  private boolean includeLoggerName = true;
  private boolean includeThreadName = true;
  private boolean includeClassName = true;

  private NoopLogger noopLogger = new NoopLogger();

  public Logger(org.slf4j.Logger slf4jLogger, FastDateFormat formatter) {
    this.slf4jLogger = slf4jLogger;
    this.formatter = formatter;
  }

  // ========================================
  // Getters and Setters
  // ----------------------------------------

  public boolean isTraceEnabled() {
    return slf4jLogger.isTraceEnabled();
  }

  public boolean isDebugEnabled() {
    return slf4jLogger.isDebugEnabled();
  }

  public boolean isInfoEnabled() {
    return slf4jLogger.isInfoEnabled();
  }

  public boolean isWarnEnabled() {
    return slf4jLogger.isWarnEnabled();
  }

  public boolean isErrorEnabled() {
    return slf4jLogger.isErrorEnabled();
  }

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
  // Log Level API
  // ----------------------------------------

  public JsonLogger trace() {
    if (slf4jLogger.isTraceEnabled()) {
      StandardJsonLogger result = new StandardJsonLogger(this.slf4jLogger, this.formatter, this.gson, TRACE_LEVEL_NAME,
          this.slf4jLogger::trace, this.slf4jLogger::trace);
      this.configureLogger(result);
      return result;
    }

    return noopLogger;
  }

  public JsonLogger debug() {
    if (slf4jLogger.isDebugEnabled()) {
      StandardJsonLogger result = new StandardJsonLogger(this.slf4jLogger, this.formatter, this.gson, DEBUG_LEVEL_NAME,
          this.slf4jLogger::debug, this.slf4jLogger::debug);
      this.configureLogger(result);
      return result;
    }

    return noopLogger;
  }

  public JsonLogger info() {
    if (slf4jLogger.isInfoEnabled()) {
      StandardJsonLogger result = new StandardJsonLogger(this.slf4jLogger, this.formatter, this.gson, INFO_LEVEL_NAME,
          this.slf4jLogger::info, this.slf4jLogger::info);
      this.configureLogger(result);
      return result;
    }

    return noopLogger;
  }

  public JsonLogger warn() {
    if (slf4jLogger.isWarnEnabled()) {
      StandardJsonLogger result = new StandardJsonLogger(this.slf4jLogger, this.formatter, this.gson, WARN_LEVEL_NAME,
          this.slf4jLogger::warn, this.slf4jLogger::warn);
      this.configureLogger(result);
      return result;
    }

    return noopLogger;
  }

  public JsonLogger error() {
    if (slf4jLogger.isErrorEnabled()) {
      StandardJsonLogger result = new StandardJsonLogger(this.slf4jLogger, this.formatter, this.gson, ERROR_LEVEL_NAME,
          this.slf4jLogger::error, this.slf4jLogger::error);
      this.configureLogger(result);
      return result;
    }

    return noopLogger;
  }

  // ========================================
  // Internal Methods
  // ----------------------------------------

  private void configureLogger(StandardJsonLogger logger) {
    logger.setIncludeClassName(this.includeClassName);
    logger.setIncludeLoggerName(this.includeLoggerName);
    logger.setIncludeThreadName(this.includeThreadName);
  }
}
