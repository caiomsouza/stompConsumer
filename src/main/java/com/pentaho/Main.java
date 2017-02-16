package com.pentaho;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.hi3project.vineyard.comm.stomp.gozirraws.Client;
import com.hi3project.vineyard.comm.stomp.gozirraws.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/* ***********************************************************************************************
 * Program connects to the Network Rail Data Feeds using the Stomp Client Listener.
 * The Listener is Thread-safe and opens separate connections per-call, per-topic.
 * *********************************************************************************************** */
public class Main implements AutoCloseable {
    @Parameter(names={"--host", "-h"}, description="Stomp Server To Connect To")
    private String host="datafeeds.networkrail.co.uk";
    @Parameter(names={"--port", "-o"}, description="Connecting Server Port")
    private int port=61618;
    @Parameter(names={"--user","-u"}, description="User", required=true)
    private String username;
    @Parameter(names={"--password", "-p"}, description="Connection password", password=true, required=true)
    private String password;
    @Parameter(names={"--help"}, description="Help", help=true, hidden=true)
    private boolean help;

    private static final Logger LOGGER = LoggerFactory.getLogger("Main");

    private List<String> topics;
    private BufferedReader reader;

    private Client client;

    private boolean init0() throws Exception {
        InputStream is = ClassLoader.getSystemResourceAsStream("topics");
        reader = new BufferedReader(new InputStreamReader(is));
        topics = reader.lines().collect(Collectors.toList());

        client = new Client(host, port, username, password);
        if (! client.isConnected()) {
            LOGGER.error("| Could not connect : " + client.toString());
            return false;
        }

        LOGGER.info("| Connected to : " + host + ":" + port);
        LOGGER.info("| Subscribing to topics : " + topics);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            client.disconnect();
            LOGGER.info("| Client Disconnected ; System Shutting Down");
        }));

        return true;
    }

    private void go() throws Exception {
        Files.createDirectories(Paths.get("output"));

        for(String topic:topics)
            client.subscribe("/topic/" + topic,new TopicListener(topic));
    }

    class TopicListener implements Listener, AutoCloseable {
        final String topic;
        final Logger logger;
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

        BufferedWriter writer;

        TopicListener(final String topic) throws Exception {
            this.topic = topic;
            this.logger = LoggerFactory.getLogger(topic);
            this.writer = new BufferedWriter(new FileWriter("output/" + topic + "_" + LocalDateTime.now().format(formatter) + ".json"));
        }

        @Override
        public void message(Map header, String body) {
            try {
                writer.write(body);
                writer.newLine();
            } catch (IOException x) {
                logger.error("| Topic | " + topic + " | Message consumption failed : " + x.toString());
            }
        }

        @Override
        public void close() throws Exception {
            writer.close();
        }
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    /* ***********************************************************************************************
     * Starting Point
     * *********************************************************************************************** */
    public static void main(String... args) throws Exception {
        Main main = new Main();
        JCommander jc = new JCommander(main, args);

        if (main.help) {
            jc.usage();
            return;
        }

        if (main.init0())
            main.go();
    }
}