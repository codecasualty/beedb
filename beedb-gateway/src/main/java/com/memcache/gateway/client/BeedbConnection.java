package com.memcache.gateway.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.RuntimeException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;

class BeedbConnection {
    private Socket       socket;
    private OutputStream outputStream;    
    private BufferedInputStream bufferedInputStream;
    private BufferedOutputStream bufferedOutputStream;
    private Logger       LOGGER = LoggerFactory.getLogger(BeedbConnection.class);

    public BeedbConnection(Socket socket) throws IOException{
        this.socket = socket;
        outputStream = socket.getOutputStream();
        bufferedInputStream = new BufferedInputStream(socket.getInputStream());
        bufferedOutputStream = new BufferedOutputStream(outputStream);
    }

    public String getStats() throws IOException{
        String command = "stats\r\n";
        byte[] bytes = command.getBytes(StandardCharsets.StandardCharsets.UTF_8);
        bufferedOutputStream.write(bytes);
        bufferedOutputStream.flush();
        // our response will end with \r\nEND so we have to read till that point
        StringBuilder builder = new StringBuilder();
        String line = readLine(bufferedInputStream);
        while(line != null && line.length() > 0){
            builder.append(line);
            builder.append("\n");
            line = readLine(bufferedInputStream);
            if(line == null || line.equals("END"))break;
        }
        // LOGGER.debug(line);
        return builder.toString();
    }

    public String getValue(String key) throws IOException{
        byte[] bytes = ("get " + key + "\r\n").getBytes(StandardCharsets.UTF_8);
        bufferedOutputStream.write(bytes);
        bufferedOutputStream.flush();
        // our response will end with \r\nEND so we have to read till that point
        StringBuilder builder = new StringBuilder();
        String header = readLine(bufferedInputStream);
        if(header == null)return null;
        String[] headerParts = header.split(" ");
        if(headerParts.length != 4){
            // these are get , append , prepend commands
            System.out.println("command not supported");
        }else{
            int length = Integer.parseInt(headerParts[3]);
            // String value = new String(bufferedReader.readNBytes(length));
            byte[] buffer = readNBytes(length , bufferedInputStream);
            String value = new String(buffer, StandardCharsets.StandardCharsets.UTF_8);
            LOGGER.debug(" string read is {}", value);
            // LOGGER.debug(value);
            // for END\r\n one extra read line is required
            String endLine = readLine(bufferedInputStream);
            LOGGER.debug(" final lien is {} " , endLine);
            return value;
        }
        return null;
    }

    public String setValue(String key, String value) throws IOException{
        byte[] bytes = ("set " + key + " 0 0 " + value.getBytes(StandardCharsets.UTF_8).length + "\r\n").getBytes(StandardCharsets.UTF_8);
        bufferedOutputStream.write(bytes);
        bufferedOutputStream.write(value.getBytes(StandardCharsets.UTF_8));
        bufferedOutputStream.write("\r\n".getBytes(StandardCharsets.UTF_8));
        bufferedOutputStream.flush();
        // our response will end with \r\nEND so we have to read till that point
        StringBuilder builder = new StringBuilder();
        String line = readLine(bufferedInputStream);
        // LOGGER.debug(line);
        return line;
    }

    public String deleteValue(String key) throws IOException{
        byte[] bytes = ("delete " + key + "\r\n").getBytes(StandardCharsets.UTF_8);
        bufferedOutputStream.write(bytes);
        bufferedOutputStream.flush();
        // our response will end with \r\nEND so we have to read till that point
        StringBuilder builder = new StringBuilder();
        String line = readLine(bufferedInputStream);
        // LOGGER.debug(line);
        return line;
    }

    public void close() throws IOException{
        bufferedInputStream.close();
        outputStream.close();
        socket.close();
    }

    public byte[] readNBytes(int n , BufferedInputStream bufferedInputStream) throws IOException{
        byte[] buffer = new byte[n];
        try{
            bufferedInputStream.read(buffer, 0, n);
        }catch(Exception e){
            LOGGER.error("readNBytes threw exception ", e);
        }
        return buffer;
    }

    public String readLine(BufferedInputStream bufferedInputStream) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while(true){
            try{
                int c = bufferedInputStream.read();
                if(c == -1 || c == '\n')break;
                baos.write(c);
            }catch(Exception e){
                LOGGER.error("readLine threw exception ", e);
                break;
            }
        }
        return baos.toString();
    }

}
