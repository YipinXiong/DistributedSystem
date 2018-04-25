package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.server.Connection;
import activitystreamer.util.Settings;

public class ClientSkeleton extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private TextFrame textFrame;
	//private Connection con;


	
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
		} catch (IOException e) {
			e.printStackTrace();
		}
		textFrame = new TextFrame();
	    start();
	}
	
	/*
	 * Send message to server	
	 */
	@SuppressWarnings("unchecked")
	public void sendActivityObject(JSONObject activityObj){
	       out.println(activityObj.toJSONString());//send message to server
	       out.flush();//Flush () indicates that the data in the buffer is forced to be sent out 
	                   //without waiting for the buffer to fill.
	       log.info("Message sent");
	       String sendcommand = (String) activityObj.get("command");
	       log.debug(sendcommand+":为什么不走下一行？");
   		   if (sendcommand == "LOGOUT") {  			
	   			try {
	   				log.debug("I am here!");
						s.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
   		   }
	}
	
	/*
	 * when the user click the disconnect button, the socket is disconnected.
	 */
	public void disconnect() {
		try {
			s.close();
			log.info("connection is disconnect");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public void run(){
		/*
		 * read message from server	 
		 */
		JSONObject resJsonObj = null;
		try {
	        String result = null;  	        
	        while((result = in.readLine()) != null) {
	        	parser = new JSONParser();
	            resJsonObj = (JSONObject) parser.parse(result);
	    		log.info("Received from server: " + resJsonObj);   
	    		textFrame.setOutputText(resJsonObj); 
	    		String command = null;
	    		command = (String) resJsonObj.get("command");
	    		log.debug(command);
				switch(command) {
				case ("REGISTER_SUCCESS"): {
					continue;
				} case ("REGISTER_FAILED"): {	
					s.close();
					ClientSkeleton c = ClientSkeleton.getInstance();
					//当注册失败时是否需要断开socket，再重连socket？ server断开连接的话，client方知道吗？
					//还能继续发送message吗？    和login failed一样的问题
				} case ("LOGIN_SUCCESS"): {
					continue;
				} case ("REDRICT"): {
					String hostname = null;
					int port = 0;
					hostname = (String) resJsonObj.get("hostname");
					port =  (int) resJsonObj.get("port");
//					Settings.setRemoteHostname(hostname);
//					Settings.setRemotePort(port);
					Settings.setLocalHostname(hostname);
					Settings.setLocalPort(port);
					ClientSkeleton c = ClientSkeleton.getInstance();
				} case ("LOGIN_FAILED"): {
					s.close();	//After sending a LOGIN_FAILED the server will close the connection.
					            //所以，client用不用关？还是当client看到server关了，自己主动点disconnect？
				} case ("INVAILD_MESSAGE"): {
					s.close();
				} case ("AUTHENTICATION_FAIL"): {
					s.close();
				} default: {
					log.info("Unkown command");
					s.close();			
				}
	            } 
	        }
	    } catch (SocketException e) {  
	       log.info("Socket closed");  
	    } catch (IOException e) {  
	        e.printStackTrace();  
	    } catch (ParseException e) {
	    	 e.printStackTrace();  
		} catch (Exception e) {
			log.error("JSON parse error while parsing message");
			e.printStackTrace();
		}		
	}
}
		 
			

