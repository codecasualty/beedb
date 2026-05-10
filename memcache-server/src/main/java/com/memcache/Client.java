package com.memcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

public class Client {

    public static void main(String[] args) throws IOException {
        try (SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", 11211));
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("Connected to server. Type a message and press Enter:");

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
                    System.out.println("Server: " + response);
                }
            }
        }
    }
}

