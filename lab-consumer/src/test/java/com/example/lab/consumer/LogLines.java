package com.example.lab.consumer;

import java.util.List;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.slf4j.LoggerFactory;

/**
 * Test-only log capture (spring-kafka-test 4.x no longer ships LogCaptor):
 * attaches a logback ListAppender to one logger and polls for matching lines.
 */
public final class LogLines implements AutoCloseable {

    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender;

    public LogLines(Class<?> clazz) {
        this.logger = (Logger) LoggerFactory.getLogger(clazz);
        this.appender = new ListAppender<>();
        this.appender.start();
        this.logger.addAppender(this.appender);
    }

    /** True when at least one captured line matches the predicate within timeoutMs. */
    public boolean awaitContains(long timeoutMs, java.util.function.Predicate<String> predicate) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (lines().stream().anyMatch(predicate)) {
                return true;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return lines().stream().anyMatch(predicate);
    }

    public List<String> lines() {
        return appender.list.stream().map(e -> e.getFormattedMessage()).collect(Collectors.toList());
    }

    @Override
    public void close() {
        this.logger.detachAppender(this.appender);
        this.appender.stop();
    }
}
