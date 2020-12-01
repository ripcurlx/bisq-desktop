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

package bisq.monitor.reporter;

import bisq.monitor.OnionParser;
import bisq.monitor.Reporter;

import bisq.network.p2p.NodeAddress;

import bisq.common.app.Version;
import bisq.common.config.BaseCurrencyNetwork;

import org.berndpruenster.netlayer.tor.TorSocket;

import com.google.common.base.Charsets;

import java.net.Socket;

import java.io.IOException;
import java.io.OutputStream;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

/**
 * Reports our findings to a graphite service.
 *
 * @author Florian Reimair
 */
@Slf4j
public class GraphiteReporter extends Reporter {
    @Nullable
    private Socket socket;

    @Override
    public void report(long value, String prefix) {
        HashMap<String, String> result = new HashMap<>();
        result.put("", String.valueOf(value));
        report(result, prefix);

    }

    @Override
    public void report(long value) {
        report(value, "");
    }

    @Override
    public void report(Map<String, String> values, String prefix) {
        String timestamp = String.valueOf(System.currentTimeMillis());
        values.forEach((key, value) -> {

            report(key, value, timestamp, prefix);
            try {
                // give Tor some slack
                // TODO maybe use the pickle protocol?
                // https://graphite.readthedocs.io/en/latest/feeding-carbon.html
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
    }

    @Override
    public void report(String key, String value, String timeInMilliseconds, String prefix) {
        log.info("report called with params: key={}, value={}, timeInMilliseconds={}, prefix={}", key, value, timeInMilliseconds, prefix);
        // https://graphite.readthedocs.io/en/latest/feeding-carbon.html
        String report = "bisq" + (Version.getBaseCurrencyNetwork() != 0 ? "-" + BaseCurrencyNetwork.values()[Version.getBaseCurrencyNetwork()].getNetwork() : "")
                + (prefix.isEmpty() ? "" : "." + prefix)
                + (key.isEmpty() ? "" : "." + key)
                + " " + value + " " + Long.parseLong(timeInMilliseconds) / 1000 + "\n";

        if (socket == null) {
            createSocket();
        }
        if (socket != null) {
            try (OutputStream outputStream = socket.getOutputStream()) {
                outputStream.write(report.getBytes(Charsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            log.debug("No socket setup. That might be expected for dev setup.");
        }
    }

    protected void createSocket() {
        try {
            NodeAddress nodeAddress = OnionParser.getNodeAddress(configuration.getProperty("serviceUrl"));
            if (nodeAddress.getFullAddress().contains(".onion")) {
                socket = new TorSocket(nodeAddress.getHostName(), nodeAddress.getPort());
            } else {
                socket = new Socket(nodeAddress.getHostName(), nodeAddress.getPort());
            }
        } catch (IOException e) {
            //e.printStackTrace();
            log.debug("Cannot create socket. That might be expected for dev setup.");
        }
    }

    @Override
    public void report(Map<String, String> values) {
        report(values, "");
    }
}
