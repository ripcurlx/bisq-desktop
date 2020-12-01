/*
 * This file is part of Bisq.
 *
 * Bisq is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at
 * your option) any later version.
 *
 * Bisq is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Bisq. If not, see <http://www.gnu.org/licenses/>.
 */

package bisq.monitor;

import bisq.monitor.metric.MarketStats;
import bisq.monitor.metric.P2PMarketStats;
import bisq.monitor.metric.P2PNetworkLoad;
import bisq.monitor.metric.P2PRoundTripTime;
import bisq.monitor.metric.P2PSeedNodeSnapshot;
import bisq.monitor.metric.PriceNodeStats;
import bisq.monitor.metric.TorHiddenServiceStartupTime;
import bisq.monitor.metric.TorRoundTripTime;
import bisq.monitor.metric.TorStartupTime;
import bisq.monitor.reporter.ConsoleReporter;
import bisq.monitor.reporter.GraphiteReporter;

import bisq.common.UserThread;
import bisq.common.app.Capabilities;
import bisq.common.app.Capability;
import bisq.common.app.Log;
import bisq.common.util.Utilities;

import org.berndpruenster.netlayer.tor.NativeTor;
import org.berndpruenster.netlayer.tor.Tor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.nio.file.Paths;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;



import sun.misc.Signal;

/**
 * Monitor executable for the Bisq network.
 *
 * @author Florian Reimair
 */
@Slf4j
public class Monitor {
    @Getter
    public static File appDir;
    @Getter
    public static File torDir;

    public static void main(String[] args) throws Throwable {
        new Monitor().start(args);
    }

    public static void shutDown() {
        log.info("Graceful shutdown started");

        // We must not call System.exit() or Runtime.getRuntime().exit() as that would lead to a deadLock as we are
        // called from the shutDownHook
        UserThread.runAfter(() -> {
            log.info("Graceful shutdown not completed after 5 sec. We exit now.");
            Runtime.getRuntime().halt(0);
        }, 10);


        log.info("Shutdown metrics...");
        Metric.haltAllMetrics();

        Tor tor = Tor.getDefault();
        if (tor != null) {
            log.info("Shutdown tor...");
            tor.shutdown();
        }

        log.info("Graceful shutdown complete");
    }

    private final List<Metric> metrics = new ArrayList<>();

    private void start(String[] args) throws Throwable {
        appDir = new File(Utilities.getUserDataDir(), "bisq-monitor");
        torDir = new File(appDir, "tor");
        log.info("App dir: {}", appDir);

        setupLog();

        setupUserThread();

        setupCapabilities();

        setupShutDownHandlers();

        // blocking call
        startTor();

        Properties properties = getProperties(args);

        Reporter reporter = "true".equals(properties.getProperty("System.useConsoleReporter", "false")) ?
                new ConsoleReporter() :
                new GraphiteReporter();

        metrics.add(new TorStartupTime(reporter));
        metrics.add(new TorRoundTripTime(reporter));
        metrics.add(new TorHiddenServiceStartupTime(reporter));
        metrics.add(new P2PRoundTripTime(reporter));
        metrics.add(new P2PNetworkLoad(reporter));
        metrics.add(new P2PSeedNodeSnapshot(reporter));
        metrics.add(new P2PMarketStats(reporter));
        metrics.add(new PriceNodeStats(reporter));
        metrics.add(new MarketStats(reporter));

        configureAllMetrics(properties, metrics);

        // prepare configuration reload
        // Note that this is most likely only work on Linux
        Signal.handle(new Signal("USR1"), signal -> {
            try {
                configureAllMetrics(getProperties(args), metrics);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private void setupLog() {
        String logPath = Paths.get(appDir.getPath(), "monitor").toString();
        Log.setup(logPath);
        log.info("Log files under: {}", logPath);
        Utilities.printSysInfo();
    }

    private void setupShutDownHandlers() {
        Signal.handle(new Signal("INT"), signal -> {
            log.info("Signal {} received. We shut down.", signal.getName());
            Monitor.shutDown();
        });
        Signal.handle(new Signal("TERM"), signal -> {
            log.info("Signal {} received. We shut down.", signal.getName());
            Monitor.shutDown();
        });

        Runtime.getRuntime().addShutdownHook(new Thread(Monitor::shutDown, "Monitor Shutdown Hook "));
    }

    private void setupUserThread() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(this.getClass().getSimpleName())
                .setDaemon(true)
                .build();
        UserThread.setExecutor(Executors.newSingleThreadExecutor(threadFactory));
    }

    private void configureAllMetrics(Properties properties, List<Metric> metrics) {
        metrics.forEach(metric -> metric.configure(properties));
    }

    private void startTor() throws org.berndpruenster.netlayer.tor.TorCtlException {
        // TODO maybe better to create on demand from inside metrics
        long ts = System.currentTimeMillis();
        Tor.setDefault(new NativeTor(torDir, null, null, false));
        log.info("Starting tor took {} ms", System.currentTimeMillis() - ts);
    }

    private void setupCapabilities() {
        //noinspection deprecation,deprecation,deprecation,deprecation,deprecation,deprecation,deprecation,deprecation
        Capabilities.app.addAll(Capability.TRADE_STATISTICS,
                Capability.TRADE_STATISTICS_2,
                Capability.ACCOUNT_AGE_WITNESS,
                Capability.ACK_MSG,
                Capability.PROPOSAL,
                Capability.BLIND_VOTE,
                Capability.DAO_STATE,
                Capability.BUNDLE_OF_ENVELOPES,
                Capability.REFUND_AGENT,
                Capability.MEDIATION,
                Capability.TRADE_STATISTICS_3);
    }


    /**
     * Overloads a default set of properties with a file if given
     *
     * @return a set of properties
     * @throws IOException in case something goes wrong
     */
    private Properties getProperties(String[] args) throws IOException {
        Properties result = new Properties();

        // if we have a config file load the config file, else, load the default config
        // from the resources
        if (args.length > 0) {
            result.load(new FileInputStream(args[0]));
        } else {
            result.load(Monitor.class.getClassLoader().getResourceAsStream("metrics.properties"));
        }

        return result;
    }
}
