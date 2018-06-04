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

import org.apache.commons.lang3.time.FastDateFormat;

/**
 * Builder for Logger instances which provides a convenient means to configure the logger that
 * will easily remain backward-compatible as additional settings are added over time.
 *
 * Created by art on 10/13/16.
 */
public class LoggerBuilder {

  private org.slf4j.Logger slf4jLogger;
  private FastDateFormat dateFormatter;
  private boolean includeLoggerName = true;
  private boolean includeThreadName = true;
  private boolean includeClassName = true;

  public LoggerBuilder slf4jLogger(org.slf4j.Logger slf4jLogger, FastDateFormat dateFormatter) {
    this.slf4jLogger = slf4jLogger;
    this.dateFormatter = dateFormatter;
    return this;
  }

//========================================
// Public API
//----------------------------------------

  public LoggerBuilder includeLoggerName(boolean includeLoggerName) {
    this.includeLoggerName = includeLoggerName;
    return this;
  }

  public LoggerBuilder includeClassName(boolean includeClassName) {
    this.includeClassName = includeClassName;
    return this;
  }

  public LoggerBuilder includeThreadName(boolean includeThreadName) {
    this.includeThreadName = includeThreadName;
    return this;
  }

  public Logger build() {
    Logger result = new Logger(this.slf4jLogger, this.dateFormatter);

    result.setIncludeThreadName(this.includeThreadName);
    result.setIncludeLoggerName(this.includeLoggerName);
    result.setIncludeClassName(this.includeClassName);

    return result;
  }
}
