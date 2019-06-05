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

package ai.tripl.arc.util.log;

import ai.tripl.arc.util.log.logger.Logger;

import org.apache.commons.lang3.time.FastDateFormat;

public class LoggerFactory {
  private static String dateFormatString = "yyyy-MM-dd HH:mm:ss.SSSZ";
  private static FastDateFormat formatter = FastDateFormat.getInstance(dateFormatString);
  private static boolean includeLoggerName = true;
  private static boolean includeThreadName = true;
  private static boolean includeClassName = true;

  // ========================================
  // Static Getters and Setters
  // ----------------------------------------

  public static void setDateFormatString(String dateFormatString) {
    LoggerFactory.dateFormatString = dateFormatString;
    LoggerFactory.formatter = FastDateFormat.getInstance(dateFormatString);
  }

  public static void setIncludeLoggerName(boolean includeLoggerName) {
    LoggerFactory.includeLoggerName = includeLoggerName;
  }

  public static void setIncludeThreadName(boolean includeThreadName) {
    LoggerFactory.includeThreadName = includeThreadName;
  }

  public static void setIncludeClassName(boolean includeClassName) {
    LoggerFactory.includeClassName = includeClassName;
  }

  public static boolean isIncludeLoggerName() {
    return includeLoggerName;
  }

  public static boolean isIncludeThreadName() {
    return includeThreadName;
  }

  public static boolean isIncludeClassName() {
    return includeClassName;
  }
  // ========================================
  // Factory API
  // ----------------------------------------

  public static Logger getLogger(String name) {
    org.slf4j.Logger slf4jLogger = org.slf4j.LoggerFactory.getLogger(name);
    Logger result = new Logger(slf4jLogger, formatter);
    configureLogger(result);

    return result;
  }

  // ========================================
  // Internal Methods
  // ----------------------------------------

  private static void configureLogger(Logger logger) {
    logger.setIncludeClassName(includeClassName);
    logger.setIncludeLoggerName(includeLoggerName);
    logger.setIncludeThreadName(includeThreadName);
  }
}
