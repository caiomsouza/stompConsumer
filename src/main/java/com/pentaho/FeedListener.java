package com.pentaho;

import com.hi3project.vineyard.comm.stomp.gozirraws.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.*;
import java.util.Map;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

public class FeedListener implements Listener, Closeable {
    private Logger logger;
    private BufferedWriter writer;

    public FeedListener(final String topic) throws IOException {
        Path p = Paths.get("output/" + topic + ".json");
        writer = Files.newBufferedWriter(p,CREATE,WRITE);

        logger = LoggerFactory.getLogger(topic);
    }

    @Override
    public void message(Map header, String body) {
        logger.debug("| Got header: " + header);
        logger.debug("| Got body: " + body);

        try {
            writer.write(body);
            writer.newLine();
        } catch (IOException x) {
            logger.error(x.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
