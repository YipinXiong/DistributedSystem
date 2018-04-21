package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.JsonParser;

import activitystreamer.Client;
import activitystreamer.server.Control;
import activitystreamer.util.Settings;

public class ClientSkeleton extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private TextFrame textFrame;
	
	private int portnum;
	private boolean term = false;
	private ServerSocket serverSocket = null;
	

	
	public static ClientSkeleton getInstance(){
		if(clientSolution==null){
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}
	
	Socket s = null;
//    DataInputStream in;
//    DataOutputStream out;
//    BufferedReader inread;
//    PrintWriter outwriter;
    
    
	BufferedReader in;
	PrintWriter out;
//	BufferedReader wt;
    JSONParser parser;
	public ClientSkeleton(){
		try {
			//s = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());
			   //客户端请求与remote端口建立TCP连接
			s = new Socket(Settings.getLocalHostname(), Settings.getLocalPort());
			log.info("Connection established");
//			in = new DataInputStream( s.getInputStream());
//			out =new DataOutputStream( s.getOutputStream());
//			inread = new BufferedReader(new InputStreamReader(in));
//			outwriter = new PrintWriter(out,true);
			in = new BufferedReader(new InputStreamReader(s.getInputStream()));
			   //获取Socket的输入流，用来接收从服务端发送过来的数据
			out = new PrintWriter( s.getOutputStream());
			   //获取Socket的输出流，用来发送数据到服务端
			//wt = new BufferedReader(new InputStreamReader(System.in));
			  //从键盘输入的数据流
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		textFrame = new TextFrame();
	    start();
	}
	
		
	@SuppressWarnings("unchecked")
	public void sendActivityObject(JSONObject activityObj){
	            out.println(activityObj.toJSONString());//发送数据到服务端
	            out.flush();//Flush () indicates that the data in the buffer is forced to be sent out 
	                           //without waiting for the buffer to fill.
	            log.info("Message sent");
	}
	
	
	public void disconnect(){
		try {
			s.close();
			log.info("connection is disconnect");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	public void run(){
		//read message from server		
//		log.info("listening for new connections on "+portnum);
//		  while(!term){
//		   Socket clientSocket;
//		   try {
//		    clientSocket = serverSocket.accept();
//		    Control.getInstance().incomingConnection(clientSocket);
//		   } catch (IOException e) {
//		    log.info("received exception, shutting down");
//		    term=true;
//		   }
//		  }
		try {
            String result = null;  
            //result = in.readLine();
            log.debug(result);
            while((result = in.readLine()) != null) {
            	JSONObject resJsonObj;
            	parser = new JSONParser();
                resJsonObj = (JSONObject) parser.parse(result);
        		log.info("Received from server: "+resJsonObj);
                textFrame.setOutputText(resJsonObj);    
           }
//            String data;
//            boolean status = true;
//            while (status && (data = in.readUTF()) != null) {
//            	try {
//            		log.debug("Receive data {}", data);
//            		JsonParser parser = new JsonParser();
//            		JsonObject json = parser.parse(data).getAsJsonObject();
//            	}
//            }
            
        } catch (SocketException e) {  
           log.info("disconnect");  
        } catch (IOException e) {  
            e.printStackTrace();  
        } catch (ParseException e) {

		}          
			
	}
//	@SuppressWarnings("unchecked")
//	public void sendRegisterMessge() {
//		log.info("want to register", Settings.getUsername(),Settings.getSecret());
//		JSONParser parser = new JSONParser();		
//
//			JSONObject msgJsonObj;
//			try {
//				msgJsonObj = (JSONObject) parser.parse(wt);
//				JSONObject newCommand = new JSONObject();
//			    String command = (String) msgJsonObj.get("command");
//			    String username = (String) msgJsonObj.get("username");
//			    String secret = (String) msgJsonObj.get("secret");
//			    newCommand.put("command", command);
//			    newCommand.put("username",username);
//			    newCommand.put("secret",secret);
//	            System.out.println(newCommand.toJSONString());
//			} catch (IOException e) {
//				
//				e.printStackTrace();
//			} catch (ParseException e) {
//				
//				e.printStackTrace();
//			}
		    

//	}
}
