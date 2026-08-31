/*
 * Copyright 2026 subodh.
 * this class opens a socket connection to local host : 11211 port where beedb-server is running
 * and then sends commands to beedb-server
 */
package com.memcache.gateway.client;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.io.OutputStreamWriter;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.RuntimeException;
import java.util.Arrays;
public class BeedbClient {

    BlockingQueue<Socket> socketQueue = new LinkedBlockingQueue<>();
    Logger LOGGER = LoggerFactory.getLogger(BeedbClient.class.getName());
    Scanner scanner = new Scanner(System.in);
    int leaderPort = 11211;
    public static void main(String[] args) throws IOException{
        BeedbClient client = new BeedbClient();
        client.start();
    }

    public void start() throws IOException{
        while(true){
            Socket socket = borrowConnection();
            LOGGER.debug("socket is {} ", socket);
            try{
                String command = scanner.nextLine();
                System.out.println(command);
                if(command.equals("exit")){
                    break;
                }
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                // String stats = getStats(printWriter, bufferedReader);
                // System.out.println(stats);
                // String value = getValue(printWriter, bufferedReader, "key1");
                // System.out.println(value);
                // setValue(printWriter, bufferedReader, "key1", "value1");
                // value = getValue(printWriter, bufferedReader, "key1");
                // System.out.println(value);
                if(command.startsWith("set")){
                    String[] parts = command.split(" ");
                    String key = parts[1];
                    String value = String.join(" ", Arrays.copyOfRange(parts, 2, parts.length));
                    String response = setValue(printWriter, bufferedReader, key, value);
                    LOGGER.debug("response from beedb-server for set is {} ", response);
                }else if(command.startsWith("get")){
                    String[] parts = command.split(" ");
                    String key = parts[1];
                    String value = getValue(printWriter, bufferedReader, key);
                    LOGGER.debug("value returned by our funciton is {} ", value);
                }else if(command.startsWith("stats")){
                    String response = getStats(printWriter, bufferedReader);
                    LOGGER.debug("response from beedb-server for stats is {} ", response);
                }else{
                    LOGGER.debug("command not supported");
                }
                putBackConnection(socket);
            }catch(Exception e){
                LOGGER.error("something is wrong {}",e);
                discardConnection(socket);
            }
        }
    }

    public Socket borrowConnection() throws IOException{
        Socket socket = socketQueue.poll();
        if(socket == null){
            socket = new Socket("localhost", leaderPort);
            socket.setSoTimeout(6000);
        }
        return socket;
    }

    public void putBackConnection(Socket socket){
        socketQueue.add(socket);
    }

    public void discardConnection(Socket socket){
        try{
            socket.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public  String getStats(PrintWriter printWriter, BufferedReader bufferedReader) throws IOException{
        printWriter.print("stats\r\n");
        printWriter.flush();
        // our response will end with \r\nEND so we have to read till that point
        StringBuilder builder = new StringBuilder();
        String line = bufferedReader.readLine();
        while(line != null && line.length() > 0){
            builder.append(line);
            builder.append("\n");
            line = bufferedReader.readLine();
            if(line.equals("END"))break;
        }
        // System.out.println(line);
        return builder.toString();
    }

    public  String getValue(PrintWriter printWriter, BufferedReader bufferedReader , String key) throws IOException{
        printWriter.print("get " + key + "\r\n");
        printWriter.flush();
        // our response will end with \r\nEND so we have to read till that point
        StringBuilder builder = new StringBuilder();
        String header = bufferedReader.readLine();
        String[] headerParts = header.split(" ");
        LOGGER.debug("header is {} length is {}", header, headerParts.length);
        if(headerParts.length != 4){
            // these are get , append , prepend commands
            System.out.println("command not supported");
        }else{
            int length = Integer.parseInt(headerParts[3]);
            // String value = new String(bufferedReader.readNBytes(length));
            char[] buffer = new char[length];
            int read = bufferedReader.read(buffer, 0 , length);
            String value = new String(buffer);
            LOGGER.debug(" string read is {} ", value);
            // System.out.println(value);
            // for END\r\n one extra read line is required
            String endLine = bufferedReader.readLine();
            LOGGER.debug(" final lien is {} ", endLine);
            return header+" "+value+""+endLine;
        }
        return "END\r\n";
    }

    public String setValue(PrintWriter printWriter, BufferedReader bufferedReader , String key, String value) throws IOException{
        printWriter.print("set " + key + " 0 0 " + value.length() + "\r\n");
        printWriter.print(value);
        printWriter.print("\r\n");
        printWriter.flush();
        // our response will end with \r\nEND so we have to read till that point
        // LOGGER.debug("set value is called, waiting for response");
        StringBuilder builder = new StringBuilder();
        String header = bufferedReader.readLine();
        // LOGGER.debug("response is {}", header);
        if(header.startsWith("SERVER_ERROR")){
            LOGGER.debug("server error is {}", header);
            String nodeId = header.split(" ")[3];
            LOGGER.debug("nodeId is {} ", nodeId);
            if(nodeId.equals("node1")) leaderPort = 11211;
            else if(nodeId.equals("node2")) leaderPort = 11212;
            else if(nodeId.equals("node3")) leaderPort = 11213;
            else leaderPort = 11214;
            throw new RuntimeException("server error");
        }
        return header;
    }

    
}
