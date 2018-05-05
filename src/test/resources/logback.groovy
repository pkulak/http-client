def PATTERN = "%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n"

appender("STDOUT", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = PATTERN
    }
}

root(INFO, ["STDOUT"])
logger("com.pkulak.httpclient", TRACE)