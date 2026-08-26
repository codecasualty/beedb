package com.memcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {

    public static void main(String[] args) throws IOException {
        Logger LOGGER = LoggerFactory.getLogger(Client.class.getName());
        int clientPort = Integer.parseInt(args[0]);
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", clientPort));
             Scanner scanner = new Scanner(System.in)) {

            LOGGER.debug("Connected to server. Type a message and press Enter:");

            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                if (line.equals("quit")) break;
                line = line.replace("\\r", "\r").replace("\\n", "\n");
                byte[] body = line.getBytes("UTF-8");
                ByteBuffer buffer = ByteBuffer.wrap(body);
                channel.write(buffer);

                ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
                int bytesRead = channel.read(responseBuffer);
                if (bytesRead > 0) {
                    responseBuffer.flip();
                    String response = new String(responseBuffer.array(), 0, bytesRead);
                    LOGGER.debug("Response from server: {}", response);
                }
            }
        }
    }
}

