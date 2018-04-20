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
	//userList is used to record registered users.
	private static HashMap<String,String> userList = new HashMap<String, String>();
	private static ArrayList<Connection> clientList = new ArrayList<Connection>();
	private static ArrayList<Connection> serverList = new ArrayList<Connection>();
	private static HashMap<String, Integer> serversLoad = new HashMap<String, Integer>();
	private static HashMap<String, String[]> serversAddr = new HashMap<String, String[]>();
	

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
			userList.put("anonymous", "THIS IS NOT ALLOWED TO REGISTERD");
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
			 serverList.add(s1);
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
	public synchronized boolean process(Connection con,String msg) {
		//loggedUser uses as flag which indicates currently logged username 
		String loggedUser = null;
		JSONParser parser = new JSONParser();
		JSONObject returnJsonObj = new JSONObject();
		try {
			JSONObject msgJsonObj = (JSONObject) parser.parse(msg);
			String command = (String) msgJsonObj.get("command");
			
			if(command==null) {
				throw new Exception();
			}
			
			if(command.equals("LOGIN")) {
				String username = (String) msgJsonObj.get("username");
				String password = (String) msgJsonObj.get("secret");
				if(username.equals("anonymous")) {
					clientList.add(con);
					loggedUser = username;
					returnJsonObj.put("command", "LOGIN_SUCCESS");
					returnJsonObj.put("info", "logged in as anonymous");
					con.writeMsg(returnJsonObj.toJSONString());
				}
				else if(username.equals(null)) {
					writeBack(con, msg, false);
					con.closeCon();
					return true;
				}
				else {
					if(userList.containsKey(username)&&password.equals(userList.get(username))) {
						loggedUser = username;
						returnJsonObj.put("command", "LOGIN_SUCCESS");
						returnJsonObj.put("info", "logged in as "+username);
						con.writeMsg(returnJsonObj.toJSONString());
					}
					else {
						returnJsonObj.put("command", "LOGIN_FAILED");
						returnJsonObj.put("info", "attempt to login with wrong secret");
						con.writeMsg(returnJsonObj.toJSONString());
						con.closeCon();
						return true;
					}
				}

				if(checkRedirect(clientList.size(), serversLoad)) {
					String[] address= minLoadServerAddrs(serversLoad, serversAddr);
					returnJsonObj.put("command", "REDIRECT");
					returnJsonObj.put("hostname", address[0]);
					returnJsonObj.put("port", Integer.parseInt(address[1]));
					con.writeMsg(returnJsonObj.toJSONString());
					clientList.remove(con);
					con.closeCon();
					return true;
				} 
				else {
					clientList.add(con);
				}
				return false;
			}
			

			
			else if(command.equals("LOGOUT")) {
				con.closeCon(); 
				clientList.remove(con);
				return true;
			}
			
			else if(command.equals("REGISTER")) {
				String username = (String) msgJsonObj.get("username");
			    String password = (String) msgJsonObj.get("secret");
			    if(username==null||password==null){
			    	throw new Exception();
			    }
			    
			    if(userList.containsKey(username)) {
			    	returnJsonObj.put("command", "REGISTER_FAILED");
	    			returnJsonObj.put("info", username+" is already registered with the system");
	    			con.writeMsg(returnJsonObj.toJSONString());
	    			return true;
			    }
			    //use locks to record how many servers return back messages
			    //just in case, what if one server does not return message back?! what should I do?
			    for(Connection server: serverList) {
			    	returnJsonObj.put("command", "LOCK_REQUEST");
	    			returnJsonObj.put("username", username);
	    			returnJsonObj.put("secret", password);
	    		    server.writeMsg(returnJsonObj.toJSONString());
			    }
			    //after receiving all LOCK_AllOWED messages returned from all such servers.
			    //respond with a Register Success
			    while(true) {
			    	if() {
			    		break
			    	}
			    }
			    returnJsonObj.put("command", "REGISTER_SUCCESS");
    			returnJsonObj.put("info", "register success for arron"+username);
    			con.writeMsg(returnJsonObj.toJSONString());
			    return false;
			}
			
			else if(command.equals("LOCK_ALLOWED")) {
				checkAuthenServer(con, serverList);
				String username = (String) msgJsonObj.get("username");
			    String password = (String) msgJsonObj.get("secret");
			    if(username==null||password==null){
			    	throw new Exception();
			    }
			    if (userList.containsKey(username)) {
				    for(Connection server: serverList) {
				    	returnJsonObj.put("command", "LOCK_DENIED");
		    			returnJsonObj.put("username", username);
		    			returnJsonObj.put("secret", password);
		    		    server.writeMsg(returnJsonObj.toJSONString());
				    }
				    return false;
			    }
			    else {
			    	for(Connection server: serverList) {
			    		if(con.equals(server)) {
			    			continue;
			    		}
				    	returnJsonObj.put("command", "LOCK_ALLOWED");
		    			returnJsonObj.put("username", username);
		    			returnJsonObj.put("secret", password);
		    		    server.writeMsg(returnJsonObj.toJSONString());
				    }
			    }
			    
			}
			
			else if(command.equals("LOCK_DENIED")) {
				checkAuthenServer(con, serverList);
				String username = (String) msgJsonObj.get("username");
			    String password = (String) msgJsonObj.get("secret");
			    if(username==null||password==null){
			    	throw new Exception();
			    }
			    if (userList.containsKey(username)) {
			    	userList.remove(username);
			    }
			    
			    for(Connection server: serverList) {
			    	if(con.equals(server)) {
			    		continue;
			    	}
			    	returnJsonObj.put("command", "LOCK_DENIED");
	    			returnJsonObj.put("username", username);
	    			returnJsonObj.put("secret", password);
	    		    server.writeMsg(returnJsonObj.toJSONString());
			    }
			    return false;
			}
			
			else if(command.equals("LOCK_REQUEST")) {
				checkAuthenServer(con, serverList);
				String username = (String) msgJsonObj.get("username");
			    String password = (String) msgJsonObj.get("secret");
			    if(username==null||password==null){
			    	throw new Exception();
			    }
			    if(userList.containsKey(username)) {
			    	returnJsonObj.put("command", "LOCK_DENIED");
	    			returnJsonObj.put("username", username);
	    			returnJsonObj.put("secret", password);
	    		    for(Connection server: serverList) {
	    			server.writeMsg(returnJsonObj.toJSONString());
			    	//server's connection still alive;
			    	return false;
	    		    }
			    }
			    else {
			    	//if local table doesn't contain this user; add it to local table, then broadcast to servers except coming one  
			    	userList.put(username, password);
			    	returnJsonObj.put("command", "LOCK_REQUEST");
	    			returnJsonObj.put("username", username);
	    			returnJsonObj.put("secret", password);
	    			for(Connection server: serverList) {
	    				if(server.equals(con)) {
	    					continue;
	    				}
		    			server.writeMsg(returnJsonObj.toJSONString());
				    	//server's connection still alive;
				    	return false;
			    }                                                                               
			    }
			}
			
			else if(command.equals("ACTIVITY_MESSAGE")) {
				String username = (String) msgJsonObj.get("username");
			    String password = (String) msgJsonObj.get("secret");
			    JSONObject activity = (JSONObject) msgJsonObj.get("activity");
			    if(username==null||password==null||activity==null){
			    	throw new Exception();
			    }
			    // autenCheck needs to check 1. user login or not(whether in clientList) 
			    // 2. check where the pair of secret and user is consisted with the logged user;
			    if(authenCheck(loggedUser, username, password, userList)) {
	    			log.info("pass the authentication!");
			    	for(Connection client: clientList) {
			    		if(client.equals(con)) {
			    			continue;
			    		}
			    		else {
			    			returnJsonObj.put("command", "ACTIVITY_BROADCAST");
			    			returnJsonObj.put("activity", activity);
			    		    client.writeMsg(returnJsonObj.toJSONString());
			    		}
			    	}
			    	for(Connection server: serverList) {
			    		if(server.equals(con)) {
			    			continue;
			    		}
			    		else {
			    			returnJsonObj.put("command", "ACTIVITY_BROADCAST");
			    			returnJsonObj.put("activity", activity);
			    		    server.writeMsg(returnJsonObj.toJSONString());
			    		}
			    	}
			    	return false;
			    }
			    else {
			    	//AUTHENTICATION_FAIL
					returnJsonObj.put("command", "AUTHENTICATION_FAIL");
					returnJsonObj.put("info", "the supplied secret is incorrect:"+ password);
					con.writeMsg(returnJsonObj.toJSONString());
					log.debug("connection Fails for aunthentication_fail");
					clientList.remove(con);
					con.closeCon();
					return true;
			    }
			}
			
			else if(command.equals("SERVER_ANNOUNCE")) {
				checkAuthenServer(con, serverList);
				for(Connection server: serverList) {
					//if announce was sent for yourself, update local lists.
					if(server.equals(con)) {
						log.info("Hey, it's me!! Try other conncetion!");
						try{
							updateServersLoad(msgJsonObj,serversLoad,serversAddr);
							log.info("updated serverLoad list!");
							} catch(Exception e) {
									log.debug("something wrong with updating");
									returnJsonObj.put("command", "INVALID_MESSAGE");
									returnJsonObj.put("info", "invaild announce parameter(s)!");
									con.closeCon();
									return true;
								}
					} //after getting announce, what should do next is send the Announce out to other connected servers.
					else {
						try {
						    log.info("Sending announce to connected "+server.getSocket().getPort());
							server.writeMsg(msg);
						} catch(Exception e){
							log.debug("something wrong with server getSocket");
							returnJsonObj.put("command", "INVALID_MESSAGE");
							returnJsonObj.put("info", "invaild announce parameter(s)!");
							con.closeCon();
							return true;
						}

					}
				}
				for (String serverID: serversLoad.keySet()){
		            String key =serverID;
		            String value = serversLoad.get(serverID).toString();  
		            log.info(key + " " + value);  
				} 
				return false;
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
	}
	
	private void checkAuthenServer(Connection con, ArrayList<Connection> serverList) throws Exception {
		// TODO Auto-generated method stub
		if(serverList.contains(con)) {
			throw new Exception();
		}
	}

	private boolean authenCheck(String loggedUser, String username, String password, HashMap<String, String> userList) {
		if(loggedUser.equals(username)&&password.equals(userList.get(username))) {
		return true;
		}
		return false;
	}

	private boolean checkRedirect(int curLoad, HashMap<String,Integer> serversLoad) {
		Map<String, Integer>sortedServersLoad = sortByValue(serversLoad);
		Map.Entry<String,Integer> entry = sortedServersLoad.entrySet().iterator().next();
		int value = entry.getValue().intValue();
		if(curLoad-2>=value) {
			return true;
		}
		return false;
	}

	private String[] minLoadServerAddrs(HashMap<String,Integer> serversLoad, HashMap<String, String[]> serversAddr) {
		Map<String, Integer>sortedServersLoad = sortByValue(serversLoad);
		Map.Entry<String,Integer> entry = sortedServersLoad.entrySet().iterator().next();
		String key = entry.getKey();
		return serversAddr.get(key);
	}

	// update lists related to servers.
	private synchronized void updateServersLoad (JSONObject msgJsonObj, HashMap<String, Integer> serversLoad,HashMap<String, String[]> serversAddr)  throws Exception {

		     String serverID = (String) msgJsonObj.get("id");
//		     log.info(serverID);
		     Number serverLoad = (Number) msgJsonObj.get("load");
//		     log.info(serverLoad);
		     String hostname = (String) msgJsonObj.get("hostname");
//		     log.info(hostname);
		     Number port = (Number) msgJsonObj.get("port");
//		     log.info(port);
		     log.info(serverID+" "+ serverLoad+" "+hostname+" "+port);
//		     serverID.equals(null)||serverLoad.equals(null)||hostname.equals(null)||port.equals(null)
		     if(serverID==null||serverLoad==null||hostname==null||port==null) {
		    	 throw new Exception();
		     }
		     serversLoad.put(serverID, new Integer(serverLoad.intValue()));
		     serversAddr.put(serverID, new String[] {hostname,Integer.toString(port.intValue())});
		
	}

	private synchronized void writeBack(Connection con, String command, boolean result) {
		// TODO Auto-generated method stub
		if(command.equals("LOGIN")) {
			System.out.println("Login Sucess");
		}
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
//				log.debug(server_announce.toJSONString());
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
			//all above things are for servers; the following one is to handle activity from 
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
