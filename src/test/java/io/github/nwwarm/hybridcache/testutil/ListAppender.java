package io.github.nwwarm.hybridcache.testutil;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ListAppender extends AppenderBase<ILoggingEvent> implements AutoCloseable {

    private final List<ILoggingEvent> events = new ArrayList<>();
    private final Logger target;

    private ListAppender(Logger target) { this.target = target; }

    public static ListAppender attach(Class<?> source) {
        Logger logger = (Logger) LoggerFactory.getLogger(source);
        ListAppender a = new ListAppender(logger);
        a.start();
        logger.addAppender(a);
        return a;
    }

    @Override protected synchronized void append(ILoggingEvent e) { events.add(e); }

    public synchronized List<String> messages() {
        return events.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
    }

    @Override public void close() { target.detachAppender(this); stop(); }
}
