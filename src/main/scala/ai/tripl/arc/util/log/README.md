note
======

This logging has been included from the https://github.com/savoirtech/slf4j-json-logger/ project but modified to fit better with cloud based logging solutions. Only minor changes have been added but things like @timestamp break the Microsoft Azure logging API.


slf4j-json-logger
======

The need for logging in an arbitrary JSON format is growing as more logging frameworks require this in order to
index fields for search and analytics.

Existing logging frameworks have poor support for the full JSON spec and often use shortcuts to extend functionality, such
as using the MDC context to map extra key/value pairs.

This is an effort to provide support for the full JSON spec while still using industry convention (slf4j API).

Logging configuration
===========
- Because this library is building the entire log message including things normally provided by the logging implementation, such as timestamp and level, the logging output should be configured to output the message verbatim and only the message.
- Example log4j2 properties configuration:
````
log4j.appender.out.layout.ConversionPattern=%m%n
````

Logging interface
===========
https://github.com/savoirtech/slf4j-json-logger/blob/master/slf4j-json-logger/src/main/java/com/savoirtech/logging/slf4j/json/logger/JsonLogger.java

You will have already specified the log level before reaching this interface.

LoggerFactory
===========
- Convention based static factory with familiar getLogger methods
- Factory contains a static field controlling the date format.  Override as desired.
- Factory also contains a static boolean field controlling output of the logger name.  Override as desired.  On by default.
- Default date format: yyyy-MM-dd HH:mm:ss.SSSZ
- Example output:
````
{"message":"It works!","level":"INFO","thread_name":"main","class":"com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests","logger_name":"com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests","@timestamp":"2016-08-18 08:40:49.550-0400"}
````

Default fields
===========
- level
- thread_name
- class
- logger_name (can be toggled on/off above)
- @timestamp (format set above)

Conventions
===========
Where applicable we have adopted the same naming and defaults conventions observed by our friends working on the JSON layout for log4j:
(https://github.com/logstash/log4j-jsonevent-layout)

Logger
===========
- Uses a builder pattern to help define the JSON structures being added to the log message
- Requires the log level as the first method called
- Simple example:
````java
import com.savoirtech.logging.slf4j.json.LoggerFactory;

   Logger logger = LoggerFactory.getLogger(this.getClass());
   logger.info()
       .message("It works!")
       .log();
````
- With collections:
````java
   Map<String, String> map = new HashMap<>();
   map.put("numberSold", "0");

   List<String> list = new ArrayList<>();
   list.add("Acme");
   list.add("Sun");

   logger.trace()
       .message("Report executed")
       .map("someStats", map)
       .list("customers", list)
       .field("year", "2016")
       .log();
````
````
{"message":"Report executed","someStats":{"numberSold":"0"},"customers":["Acme","Sun"],"year":"2016","level":"TRACE","thread_name":"main","class":"com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests","logger_name":"com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests","@timestamp":"2016-08-18 12:17:56.308-0400"}
````
- Gson is used to serialize objects.  The collections and objects passed in can be arbitrarily complex and/or custom.
- This library supports lambdas in order to lazily evaluate the logged objects and only evaluate if the log level is enabled.  Should be used when the log information is expensive to generate.
````java
   logger.error()
       .message(() -> "Something expensive")
       .log();
````
- .message() is a convenience method for .field("message", "your message")
- Information placed in the MDC will be logged under a top level "MDC" key in the JSON structure.  Care should be taken
to not set a field, map or list at this key as it will be overwritten.
````java
    Map<String, String> map = new HashMap<>();
    map.put("TTL", "90000");
    map.put("persistenceTime", "30000");

    MDC.put("caller", "127.0.0.1");

    logger.info()
        .message("Service trace")
        .map("someStats", map)
        .log();
````
````
{"message":"Service trace","someStats":{"persistenceTime":"30000","TTL":"90000"},"level":"INFO","thread_name":"main","class":"com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests","logger_name":"com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests","@timestamp":"2016-08-18 12:18:32.108-0400","mdc":{"caller":"127.0.0.1"}}
````
- Exceptions will be formatted with Apache Commons ExceptionUtils.
````java
    logger.error()
        .exception("myException", new RuntimeException("Something bad"))
        .log();
````
````
{"myException":"java.lang.RuntimeException: Something bad\n\tat com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests.deleteMe(BasicLoggingTests.java:47)\n\tat  ... \n","level":"ERROR","thread_name":"main","class":"com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests","logger_name":"com.savoirtech.logging.slf4j.json.logger.BasicLoggingTests","@timestamp":"2016-08-18 12:19:23.101-0400"}
````
- The stack() method allows easy output of the current stack.  Useful for code that can be reached through many paths.
````java
    logger.info()
        .stack()
        .message("Some message")
        .log();
````
````
{"stacktrace":"com.savoirtech.logging.slf4j.json.logger.StackTest$Class4.logMe(StackTest.java:79)\n\tat com.savoirtech.logging.slf4j.json.logger.StackTest$Class3.logMe(StackTest.java:69) ... ","message":"Some message","level":"INFO","thread_name":"main","class":"com.savoirtech.logging.slf4j.json.logger.StackTest$Class4","@timestamp":"2016-08-18 12:47:18.306-0400"}
````

Caution on logging numbers in JSON
===========
- Log monitoring/management applications like Loggly and Logstash require number fields to be actual number values in the JSON rather than String (single/double quoted) numbers.
````java
    logger.info()
        .field("Number that is not a number - avoid this", "1.042")
        .log();
````
````
{"Number as a string":"1.042", ...
````
````java
    double myDouble = 10.0/3.0;
    logger.info()
        .field("Some timing metric - do this instead", myDouble)
        .log();
````
````
{"Some timing metric":3.3333333333333335, ...
````

OSGi - Karaf
===========
- The bundle can be installed directly
````
bundle:install -s mvn:com.savoirtech.logging/slf4j-json-logger/2.0.1
````
- or using the features file
````
feature:repo-add mvn:com.savoirtech.logging/osgi-features/2.0.1/xml/features
feature:install slf4j-json-logger-all
````


Markers
===========
Support for markers was added with a pull request by Boris Smidt (many thanks!).
````java
    logger.info()
        .marker(MarkerFactory.getMarker("PERF"))
        .message("start() call duration")
        .field("duration", elapsedMs)
        .log();
````
````
{"marker":"PERF","message":"start() call duration","duration":351,"level":"INFO", ...}
````
