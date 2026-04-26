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

                byte[] body = line.getBytes();
                ByteBuffer buffer = ByteBuffer.allocate(4 + body.length);
                buffer.putInt(body.length);
                buffer.put(body);
                buffer.flip();
                channel.write(buffer);

                ByteBuffer responseHeader = ByteBuffer.allocate(4);
                channel.read(responseHeader);
                responseHeader.flip();
                int responseSize = responseHeader.getInt();

                ByteBuffer responseBody = ByteBuffer.allocate(responseSize);
                channel.read(responseBody);
                responseBody.flip();
                System.out.println("Server: " + new String(responseBody.array(), 0, responseBody.limit()));
            }
        }
    }
}

