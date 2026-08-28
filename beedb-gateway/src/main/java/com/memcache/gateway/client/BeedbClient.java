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
public class BeedbClient {

    public static void main(String[] args) {
        try(Socket socket = new Socket("localhost", 11211)){
            socket.setSoTimeout(6000);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

            printWriter.print("stats\r\n");
            printWriter.flush();
            // our response will end with \r\nEND so we have to read till that point
            String line = bufferedReader.readLine();
            StringBuilder builder = new StringBuilder();
            while(line != null && line.length() > 0){
                builder.append(line);
                builder.append("\n");
                line = bufferedReader.readLine();
                System.out.println(line);
                if(line.equals("END"))break;
            }
            System.out.println(builder.toString()); 
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    
}
