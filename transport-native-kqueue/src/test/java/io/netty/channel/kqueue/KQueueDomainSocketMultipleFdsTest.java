/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.kqueue;

import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.tests.UnixTestUtils;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class KQueueDomainSocketMultipleFdsTest {

    // Connects to the unix domain socket given as first argument and sends the write ends of two pipes within a
    // single SCM_RIGHTS message (more than recvFd() can accept). Afterwards it reports if the receiver closed the
    // first of these fds again, which is visible as EOF on the read end of the pipe.
    private static final String SENDER_SCRIPT =
            "import array, os, select, socket, sys\n" +
            "s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)\n" +
            "s.connect(sys.argv[1])\n" +
            "r1, w1 = os.pipe()\n" +
            "r2, w2 = os.pipe()\n" +
            "s.sendmsg([b'x'], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [w1, w2]))])\n" +
            "os.close(w1)\n" +
            "os.close(w2)\n" +
            "print('sent')\n" +
            "sys.stdout.flush()\n" +
            "ready = select.select([r1], [], [], 10)[0]\n" +
            "print('closed' if ready and os.read(r1, 1) == b'' else 'leaked')\n" +
            "sys.stdout.flush()\n" +
            "sys.stdin.read()\n";

    @BeforeAll
    public static void loadJNI() {
        KQueue.ensureAvailability();
    }

    @Test
    @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
    public void testRecvFdClosesFdsIfPeerSendsMultipleFds() throws Exception {
        assumeTrue(isPythonAvailable(), "python3 is needed to send more than one fd within a single message");

        DomainSocketAddress address = UnixTestUtils.newDomainSocketAddress();
        BsdSocket server = BsdSocket.newSocketDomain();
        BsdSocket accepted = null;
        Process sender = null;
        int received = 0;
        try {
            server.bind(address);
            server.listen(1);

            sender = new ProcessBuilder("python3", "-c", SENDER_SCRIPT, address.path())
                    .redirectErrorStream(true).start();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(sender.getInputStream(), CharsetUtil.US_ASCII));
            assertEquals("sent", reader.readLine());

            accepted = new BsdSocket(accept(server));
            IOException error = null;
            try {
                received = accepted.recvFd();
            } catch (IOException e) {
                error = e;
            }
            String verdict = reader.readLine();

            assertNotNull(error, "recvFd() must fail if more than one fd is received, but returned " + received);
            assertEquals("closed", verdict, "recvFd() leaked a received fd");
        } finally {
            if (received > 0) {
                new FileDescriptor(received).close();
            }
            if (accepted != null) {
                accepted.close();
            }
            server.close();
            if (sender != null) {
                sender.getOutputStream().close();
                sender.waitFor();
            }
        }
    }

    private static int accept(BsdSocket server) throws Exception {
        byte[] addr = new byte[64];
        for (;;) {
            int fd = server.accept(addr);
            if (fd >= 0) {
                return fd;
            }
            Thread.sleep(10);
        }
    }

    private static boolean isPythonAvailable() {
        try {
            Process process = new ProcessBuilder("python3", "-c", "import socket; socket.socket.sendmsg")
                    .redirectErrorStream(true).start();
            process.getOutputStream().close();
            return process.waitFor() == 0;
        } catch (Exception ignore) {
            return false;
        }
    }
}
