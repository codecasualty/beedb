/*
* Main Server class for memcache server
* Author : Subodh
* Date   : 2022-01-10
* mail   : trycodeforfun@gmail.com
 */

package com.memcache;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.Buffer;

public class Server {

    public static void main(String[] args) throws IOException{

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(11211);
            System.out.println("Server started on port 11211");
        } catch (Exception e) {
            System.out.println("Error starting server.. \n" + e.getMessage());
            e.printStackTrace();
        }

        Socket clientSocket = null;
        try {
            clientSocket = serverSocket.accept();
            System.out.println("Client connected");
        } catch (Exception e) {
            System.out.println("Error accepting client.. \n" + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Client request received");
        BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
        
        String input = null ;
        while((input = in.readLine()) != null) {
            System.out.println("Received request : " + input);
            out.write(input);
            out.flush();
        }

        try {
            clientSocket.close();
            serverSocket.close();
        } catch (Exception e) {
            System.out.println("Error closing socket.. \n" + e.getMessage());
            e.printStackTrace();
        }
        
    }
    
}
