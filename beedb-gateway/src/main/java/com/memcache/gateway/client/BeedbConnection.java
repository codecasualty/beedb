package com.memcache.gateway.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;

class BeedbConnection {
    private Socket       socket;
    private BufferedInputStream bufferedInputStream;
    private BufferedOutputStream bufferedOutputStream;
    private Logger       LOGGER = LoggerFactory.getLogger(BeedbConnection.class);

    public BeedbConnection(Socket socket) throws IOException{
        this.socket = socket;
        bufferedInputStream = new BufferedInputStream(socket.getInputStream());
        bufferedOutputStream = new BufferedOutputStream(socket.getOutputStream());
    }

    public String getStats() throws IOException{
        String command = "stats\r\n";
        byte[] bytes = command.getBytes(StandardCharsets.UTF_8);
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
            LOGGER.debug("command not supported");
        }else{
            int length = Integer.parseInt(headerParts[3]);
            // String value = new String(bufferedReader.readNBytes(length));
            byte[] buffer = bufferedInputStream.readNBytes(length);
            String value = new String(buffer, StandardCharsets.UTF_8);
            // LOGGER.debug(" string read is {}", value);
            // LOGGER.debug(value);
            // for END\r\n one extra read line is required
            String endLine = readLine(bufferedInputStream);
            while(endLine != null  && !endLine.equals("END")){
                endLine = readLine(bufferedInputStream);
            }
            // LOGGER.debug(" final lien is {} " , endLine);
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

    public void close(){
        try{
            bufferedInputStream.close();
            socket.close();
        }catch(Exception e){
            LOGGER.error("something is wrong ",e);
        }
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
        byte[] buffer = baos.toByteArray();
        if(buffer.length > 0 && buffer[buffer.length-1] == '\r')return new String(buffer, 0, buffer.length-1, StandardCharsets.UTF_8);
        return null;
    }

}
