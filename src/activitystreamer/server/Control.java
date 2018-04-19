package activitystreamer.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import activitystreamer.util.Settings;
 

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ArrayList<Connection> connections;
	private static boolean term=false;
	private static Listener listener;
	private static HashMap<String,String> userList = new HashMap<String, String>();
	private static ArrayList<Connection> clientList = new ArrayList<Connection>();
	private static ArrayList<Connection> serverList = new ArrayList<Connection>();
	private static HashMap<String, Integer> serversLoad = new HashMap<String, Integer>();
	private static HashMap<String, String[]> serversAddr = new HashMap<String, String[]>();
	//
	protected static Control control = null;
	
	public static Control getInstance() {
		if(control==null){
			control=new Control();
		} 
		return control;
	}
	
	public Control() {
		// initialize the connections array
		connections = new ArrayList<Connection>();
		// start a listener
		try {
			listener = new Listener();
			Settings.setSeverID();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: "+e1);
			System.exit(-1);
		}	
	}
	
	@SuppressWarnings("unchecked")
	public void initiateConnection(){
		// make a connection to another server if remote hostname is supplied
		if(Settings.getRemoteHostname()!=null){
			try {
			 Connection	s1 = outgoingConnection(new Socket(Settings.getRemoteHostname(),Settings.getRemotePort()));
			 JSONObject jsonObj = new JSONObject();
			 jsonObj.put("command", "AUTHENTICATE");
			 jsonObj.put("secret", Settings.getSecret());
			 s1.writeMsg(jsonObj.toJSONString());
			} catch (IOException e) {
				log.error("failed to make connection to "+Settings.getRemoteHostname()+":"+Settings.getRemotePort()+" :"+e);
				System.exit(-1);
			}
		}
	}
	
	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	@SuppressWarnings("unchecked")
	public synchronized boolean process(Connection con,String msg){
		JSONParser parser = new JSONParser();
		JSONObject returnJsonObj = new JSONObject();
		try {
			JSONObject msgJsonObj = (JSONObject) parser.parse(msg);
			String command = (String) msgJsonObj.get("command");
			//if no command, throw INVAID_MESSAGE;
			//judge from command to know which the connection is from client or server.
			/*If it is a client, add to the clientList
			 * If it is a server, add to the serverList*/
			if(command.equals("LOGIN")) {
				String username = (String) msgJsonObj.get("username");
				String password = (String) msgJsonObj.get("secret");
				if(inUserList(username)&&userList.get(username)==password) {
					//handle writing back login success message.
					writeBack(con, command, true);
					return false;
				}
				else {
					writeBack(con, command, false);
					con.closeCon();
					return true;
				}
			}
			
			else if(command.equals("LOGOUT")) {
				con.closeCon();
				return true;
			}
			
			
			else if(command.equals("SERVER_ANNOUNCE")) {
				log.debug(msg);
//						try{
//							updateServersLoad(msgJsonObj,serversLoad,serversAddr);
//							log.info("updated serverLoad list!");
//							return false;
//						} catch(Exception e) {
//							log.debug("something wrong with updating");
//							returnJsonObj.put("command", "INVALID_MESSAGE");
//							returnJsonObj.put("info", "update error");
//							con.closeCon();
//							return true;
//						}
				}

					
		
			
			else if(command.equals("ACTIVITY_MESSAGE")) {
				//authentication is missing here
				//write messages to every client connected to this server
				//here, it should have been added an outer loop of server list. 
				for(Connection client : clientList) {
					client.writeMsg(msg);
				}
			}
			
			else if(command.equals("INVALID_MESSAGE")) {
				String errInfo = (String) msgJsonObj.get("info");
				log.error(errInfo);
				con.closeCon();
				return true;
			}
			
			else if(command.equals("AUTHENTICATE")) {
				String secret = (String) msgJsonObj.get("secret");
				
				if(secret.equals(Settings.getSecret())) {
					log.info("Successful connection!");
					serverList.add(con);
					return false;
				} else {
					returnJsonObj.put("command", "AUTHENTICATION_FAIL");
					returnJsonObj.put("info", "the supplied secret is incorrect:"+ secret);
					con.writeMsg(returnJsonObj.toJSONString());
					log.debug("connection Fails");
					con.closeCon();
					return true;
				}
			}
			
			else if(command.equals("AUTHENTICATION_FAIL")) {
				String errInfo = (String) msgJsonObj.get("info");
				log.debug(errInfo);
				con.closeCon();
				return true;
			}
			else {
				returnJsonObj.put("command", "INVALID_MESSAGE");
				returnJsonObj.put("info", "the received message did not contain a vaild command");
				con.closeCon();
				return true;
			}
		} catch (Exception e) {
			returnJsonObj.put("command", "INVALID_MESSAGE");
			returnJsonObj.put("info","JSON parse error while parsing message");
			con.closeCon();
			return true;
		} 
		return true;
	}
	
	private synchronized void updateServersLoad (JSONObject msgJsonObj, HashMap<String, Integer> serversLoad,HashMap<String, String[]> serversAddr) throws Exception {
		// TODO Auto-generated method stub
		     String serverID = (String) msgJsonObj.get("id");
		     Integer serverLoad = (Integer) msgJsonObj.get("load");
		     String hostname = (String) msgJsonObj.get("Hostname");
		     String port = (String) msgJsonObj.get("port");
		     log.info(serverID+" "+ serverLoad+" "+hostname+" "+port);
		     if(serverID.equals(null)||serverLoad==null||hostname.equals(null)||port.equals(null)) {
		    	 throw new Exception();
		     }
		     serversLoad.put(serverID, serverLoad);
		     serversAddr.put(serverID, new String[] {hostname,port});
		
	}

	private synchronized void writeBack(Connection con, String command, boolean result) {
		// TODO Auto-generated method stub
		if(command.equals("LOGIN")) {
			System.out.println("Login Sucess");
		}
	}

	private synchronized boolean inUserList(Object object) {
		// TODO Auto-generated method stub
		return false;
	}

	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con){
		if(!term) connections.remove(con);
	}
	
	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException{
		log.debug("incomming connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}
	
	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException{
		log.debug("outgoing connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void run(){
		log.info("using activity interval of "+Settings.getActivityInterval()+" milliseconds");
		while(!term){
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
				JSONObject server_announce = new JSONObject();
				server_announce.put("command", "SERVER_ANNOUNCE");
				server_announce.put("id", Settings.getServerID());
				server_announce.put("load", clientList.size()); 
				server_announce.put("hostname",InetAddress.getLocalHost().toString());
				server_announce.put("port", Settings.getLocalPort());
				log.debug(server_announce.toJSONString());
				for(Connection server : serverList) {
					server.writeMsg(server_announce.toJSONString());
				}
//				log.info(server_announce.toString());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				log.error("Unknown Host IP");
			}
			if(!term){
				log.debug("doing activity");
				term=doActivity();
			}
			
		}
		log.info("closing "+connections.size()+" connections");
		// clean up
		for(Connection connection : connections){
			connection.closeCon();
		}
		listener.setTerm(true);
	}
    
	public static synchronized <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
        List<Entry<K, V>> list = new ArrayList<>(map.entrySet());
        list.sort(Entry.comparingByValue());
        Map<K, V> result = new LinkedHashMap<>();
        for (Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }

	
	public boolean doActivity(){
		return false;
	}
	
	public final void setTerm(boolean t){
		term=t;
	}
	
	public final ArrayList<Connection> getConnections() {
		return connections;
	}
}
