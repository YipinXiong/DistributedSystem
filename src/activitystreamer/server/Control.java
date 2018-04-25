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
	//registeredList is used to record registered users.
	private static HashMap<String,String> registeredList = new HashMap<String, String>();
	private static ArrayList<Connection> clientList = new ArrayList<Connection>();
	private static ArrayList<Connection> serverList = new ArrayList<Connection>();
	private static HashMap<String, Integer> serversLoad = new HashMap<String, Integer>();
	private static HashMap<String, String[]> serversAddr = new HashMap<String, String[]>();
	private static HashMap<String, Connection> registerReq = new HashMap<String, Connection>();
	private static int allowCount = 0;
	private static HashMap<Connection,String> loggedUser = new HashMap<Connection,String>();
	//use registerReq to record which client sent the register_request.
	public final static int HOSTNAME = 0;
	public final static int PORT =1;
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
			registeredList.put("anonymous", "THIS IS NOT ALLOWED TO REGISTERD");
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
		registeredList.put("anonymous", "anysecret");
		JSONParser parser = new JSONParser();
		JSONObject returnJsonObj = new JSONObject();
		try {
			JSONObject msgJsonObj = (JSONObject) parser.parse(msg);
			String command = (String) msgJsonObj.get("command");
			if(command==null) {
				throw new Exception("Your command cannot be null.");
			}
			switch(command) {
			
			case("LOGIN"): {
				return login(con,msgJsonObj,returnJsonObj);
			}
			
            case ("LOGOUT"): {
            	checkAuthenClient(con, clientList);
                return logout(con);
            }
			
            case("REGISTER"): {
            	return register(con,msgJsonObj,returnJsonObj);
            }
            
            case("LOCK_ALLOWED"): {
            	checkAuthenServer(con, serverList);
            	return lockAllowed(con,msgJsonObj,returnJsonObj);
            }
            
            case("LOCK_DENIED"): {
            	checkAuthenServer(con, serverList);
            	return lockDenied(con,msgJsonObj,returnJsonObj);
            }
            
            case("LOCK_REQUEST"): {
            	checkAuthenServer(con, serverList);
            	return lockRequest(con,msgJsonObj,returnJsonObj);
            }
            
            case("ACTIVITY_MESSAGE"): {
            	checkAuthenClient(con, clientList);
            	return activityMessage(con,msgJsonObj,returnJsonObj);
            }
            
            case("ACTIVITY_BROADCAST"): {
            	checkAuthenServer(con, serverList);
            	return activityBroadcast(con,msgJsonObj,returnJsonObj);
            }
            
            case("SERVER_ANNOUNCE"): {
            	checkAuthenServer(con, serverList);
            	return serverAnnounce(con,msgJsonObj,returnJsonObj, msg);
            }
            
            case("INVALID_MESSAGE"): {
            	return invalidMessage(con,msgJsonObj);
            }
            
            case("AUTHENTICATE"): {
            	return authenticate(con,msgJsonObj,returnJsonObj);
            }
            
            case("AUTHENTICATION_FAIL"): {
            	return authenticateFail(con,msgJsonObj,returnJsonObj);
            	
            }
            
            default: {
            	throw new Exception("the received message did not contain a vaild command");
            }
            
            }
			
		} catch (Exception e) {
			returnJsonObj.put("command", "INVALID_MESSAGE");
			returnJsonObj.put("info",e.getMessage());
			con.closeCon();
			return true;
		}
	}
	
	private boolean authenticateFail(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) {
		String errInfo = (String) msgJsonObj.get("info");
		log.debug(errInfo);
		con.closeCon();
		return true;
	}

	@SuppressWarnings("unchecked")
	private boolean authenticate(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) {
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

	private boolean invalidMessage(Connection con, JSONObject msgJsonObj) {
		String errInfo = (String) msgJsonObj.get("info");
		log.error(errInfo);
		con.closeCon();
		return true;
	}

	private boolean serverAnnounce(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj, String msg) throws Exception {
		for(Connection server: serverList) {
			if(server.equals(con)) {
				updateServersLoad(msgJsonObj,serversLoad,serversAddr);
			} 
			//after getting announce, what should do next is send the Announce out to other connected servers.
			else {
				server.writeMsg(msg);
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean activityBroadcast(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) throws Exception {
		
		JSONObject activity = (JSONObject) msgJsonObj.get("activity");
		activity.put("authenticated_user",msgJsonObj.get("username"));
//		String activity = (String) msgJsonObj.get("activity");
		if(msgJsonObj.get("activity")==null) {
			throw new Exception("broadactivity is incompleted");
		}
		for(Connection client: clientList) {
			returnJsonObj.put("command", "ACTIVITY_BROADCAST");
			returnJsonObj.put("activity", activity);
			client.writeMsg(returnJsonObj.toJSONString());
		}
		for(Connection server: serverList) {
			if(server.equals(con)) {
				continue;
			}
			returnJsonObj.put("command", "ACTIVITY_BROADCAST");
			returnJsonObj.put("activity", activity);
			server.writeMsg(returnJsonObj.toJSONString());
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean activityMessage(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) throws Exception {
		if(!clientList.contains(con)) {
			throw new Exception("You should Login in first");
		}
		String username = (String) msgJsonObj.get("username");
		String password = (String) msgJsonObj.get("secret");
	    JSONObject activity = (JSONObject) msgJsonObj.get("activity");
//		String activity = (String) msgJsonObj.get("activity");
		if(!username.equals("anonymous")) {
		    log.debug(username,password,activity);
		    if(username==null||password==null||activity==null){
		    	throw new Exception("ACTIVITY_MESSAGE: the json object is not completed.");
		    }
		}
		
	    log.debug(username+" "+password);
	    if(checkLoginState(loggedUser, username, password, registeredList, con)) {
			log.info("pass the authentication!");
	    	for(Connection client: clientList) {
//	    		if(client.equals(con)) {
//	    			continue;
//	    		}
//	    		else {
	    			returnJsonObj.put("command", "ACTIVITY_BROADCAST");
	    			returnJsonObj.put("activity", activity);
	    		    client.writeMsg(returnJsonObj.toJSONString());
//	    		}
	    	}
	    	for(Connection server: serverList) {
	    			returnJsonObj.put("command", "ACTIVITY_BROADCAST");
	    			returnJsonObj.put("activity", activity);
	    		    server.writeMsg(returnJsonObj.toJSONString());
	    	}
	    	return false;
	    }
	    else {
	    	//AUTHENTICATION_FAIL
			returnJsonObj.put("command", "AUTHENTICATION_FAIL");
			returnJsonObj.put("info", "the supplied user or secret is incorrect");
			con.writeMsg(returnJsonObj.toJSONString());
			log.debug("connection Fails for aunthentication_fail");
			clientList.remove(con);
			con.closeCon();
			return true;
	    }
	}

	@SuppressWarnings("unchecked")
	private boolean lockRequest(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) throws Exception {
		log.debug("recevied LOCK_REQUEST From "+ con.getSocket().toString());

		String username = (String) msgJsonObj.get("username");
	    String password = (String) msgJsonObj.get("secret");
//	    log.debug(username+" "+password);
	    if(username==null||password==null){
	    	throw new Exception("invaid parameters from LOCK_REQUEST");
	    }
	    if(registeredList.containsKey(username)) {
	    	returnJsonObj.put("command", "LOCK_DENIED");
			returnJsonObj.put("username", username);
			returnJsonObj.put("secret", password);
		    con.writeMsg(returnJsonObj.toJSONString());

	    	return false;
	    }

	    	//if local table doesn't contain this user; add it to local table, then broadcast to servers except coming one  
	    	
	        registeredList.put(username, password);
//	        log.debug(username + " "+ registeredList.get(username));

			for(Connection server: serverList) {
				if(con.equals(server)) {
					continue;
				}
		    	returnJsonObj.put("command", "LOCK_REQUEST");
    			returnJsonObj.put("username", username);
    			returnJsonObj.put("secret", password);
    			server.writeMsg(returnJsonObj.toJSONString());	
	    }
			for(Connection server: serverList) {
				JSONObject returnJsonObj2 = new JSONObject();
				//put will update before command and send allowed again
		    	returnJsonObj2.put("command", "LOCK_ALLOWED");
    			returnJsonObj2.put("username", username);
    			returnJsonObj2.put("secret", password);
    			server.writeMsg(returnJsonObj2.toJSONString());	
			}
			return false;
	}

	@SuppressWarnings("unchecked")
	private boolean lockDenied(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) throws Exception {

		String username = (String) msgJsonObj.get("username");
	    String password = (String) msgJsonObj.get("secret");
	    if(username==null||password==null){
	    	throw new Exception();
	    }
	    
	    if (registeredList.containsKey(username)) {
	    	registeredList.remove(username);
	    }
	    
	    if(registerReq.containsKey(username)) {
	    	Connection registerClient = registerReq.get(username);
	    	returnJsonObj.put("command", "REGISTER_FAILED");
			returnJsonObj.put("info", username+" is already registered with the system.");
			registerClient.writeMsg(returnJsonObj.toJSONString());
			//remove the key from HashMap. 
			con.closeCon();
			registerReq.remove(username);
	    	return true;
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

	@SuppressWarnings("unchecked")
	private boolean lockAllowed(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) throws Exception {

		String username = (String) msgJsonObj.get("username");
	    String password = (String) msgJsonObj.get("secret");
	    if(username==null||password==null){
	    	throw new Exception("LOCK_ALLOWED: parameters are not allowed to be null");
	    }
//	    log.debug("received ALLOWED from "+con.getSocket().toString());
	    
	    allowCount++;
	    //Except for this coming connection(server), broadcasts LOCK_ALLOWED to all adjacent servers. 
	    for(Connection server: serverList) {
//	    	log.debug("here!!!!!!!!!!!!!!!!!");
	    	if(server.equals(con)) {
	    		continue;
	    	}
	    	returnJsonObj.put("command", "LOCK_ALLOWED");
			returnJsonObj.put("username", username);
			returnJsonObj.put("secret", password);
		    server.writeMsg(returnJsonObj.toJSONString());
	    }
    	if(allowCount==serversLoad.size()) {
    		if(registerReq.containsKey(username)) {
			    returnJsonObj.put("command", "REGISTER_SUCCESS");
    			returnJsonObj.put("info", "register success for "+username);
    			registerReq.get(username).writeMsg(returnJsonObj.toJSONString());
    			//remove the key from HashMap. 
    			registerReq.remove(username);
    		}
    		//No matter what is original request_lock sender; You should resume allowed counter.
			allowCount=0;	
    }

	    return false;
	}

	@SuppressWarnings("unchecked")
	private boolean register(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) throws Exception {
		String username = (String) msgJsonObj.get("username");
	    String password = (String) msgJsonObj.get("secret");
	    if(username==null||password==null){
	    	throw new Exception("Register username or secret is null!!");
	    }
	    
	    if(registeredList.containsKey(username)) {
	    	returnJsonObj.put("command", "REGISTER_FAILED");
			returnJsonObj.put("info", username+" is already registered with the system");
			con.writeMsg(returnJsonObj.toJSONString());
			clientList.remove(con);
			con.closeCon();
			return true;
	    }
	    //In this way, we can get which client sent this request in order to send back Req_Fail/Success.
	    if(serverList.size()==0) {
	    	clientList.add(con);
	    	registeredList.put(username, password);
		    returnJsonObj.put("command", "REGISTER_SUCCESS");
			returnJsonObj.put("info", "register success for "+username);
			con.writeMsg(returnJsonObj.toJSONString());
	    	return false;
	    }
	    registerReq.put(username, con);
	    registeredList.put(username, password);
	    //broadcast lock_req to all adjacent clients.
	    for(Connection server: serverList) {
	    	returnJsonObj.put("command", "LOCK_REQUEST");
			returnJsonObj.put("username", username);
			returnJsonObj.put("secret", password);
		    server.writeMsg(returnJsonObj.toJSONString());
	    }
	    return false;
	}

	private boolean logout(Connection con) {
		clientList.remove(con);
		con.closeCon(); 
		loggedUser.remove(con);
		return true;
	}

	@SuppressWarnings("unchecked")
	private boolean login(Connection con, JSONObject msgJsonObj, JSONObject returnJsonObj) throws Exception {
		String username = (String) msgJsonObj.get("username");
		
		if (username==null) {
			throw new Exception("Username cannot be null!");
		}
		
		if (username.equals("anonymous")) {
			clientList.add(con);
			loggedUser.put(con, username);
			returnJsonObj.put("command", "LOGIN_SUCCESS");
			returnJsonObj.put("info", "logged in as anonymous");
			con.writeMsg(returnJsonObj.toJSONString());
		}
		//not anonymous
		else {
			String password = (String) msgJsonObj.get("secret");
			if(password==null) {
				throw new Exception("Please provide corresponding secret!");
			}
			if(registeredList.containsKey(username)&&password.equals(registeredList.get(username))) {
				clientList.add(con);
				loggedUser.put(con, username);
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
			// if it needs to redirect, removing this connection from the clientList
			clientList.remove(con);
			String[] address= minLoadServerAddrs(serversLoad, serversAddr);
			returnJsonObj.put("command", "REDIRECT");
			returnJsonObj.put("hostname", address[HOSTNAME]);
			returnJsonObj.put("port", Integer.parseInt(address[PORT]));
			con.writeMsg(returnJsonObj.toJSONString());
			con.closeCon();
			return true;
		} 
		return false;
	}

	private void checkAuthenServer(Connection con, ArrayList<Connection> serverList) throws Exception {
		if(!serverList.contains(con)) {
			throw new Exception("This is an invaild server. Authentication first");
		}
	}
	
	private void checkAuthenClient(Connection con, ArrayList<Connection> clientList) throws Exception {
		if(!clientList.contains(con)) {
			throw new Exception("This is an invaild client. Login first");
		}
	}

	private boolean checkLoginState(HashMap<Connection,String> loginUser, String username, String password, HashMap<String, String> registeredList, Connection con) {
		if(username.equals("anonymous")&&loginUser.get(con).equals("anonymous")) {
			return true;
		}
		if(username.equals(loginUser.get(con))&&password.equals(registeredList.get(username))) {
			log.debug(username+" broadcast a message.");
			return true;
		}
		log.debug("password or username are inconsistent with your log in user.");
		return false;
	}

	private boolean checkRedirect(int curLoad, HashMap<String,Integer> serversLoad) {
		Map<String, Integer>sortedServersLoad = sortByValue(serversLoad);
		Map.Entry<String,Integer> entry = sortedServersLoad.entrySet().iterator().next();
		int value = entry.getValue().intValue();
		if((curLoad-2)>value) {
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
//		     log.debug(serverID+" "+ serverLoad+" "+hostname+" "+port);
//		     serverID.equals(null)||serverLoad.equals(null)||hostname.equals(null)||port.equals(null)
		     if(serverID==null||serverLoad==null||hostname==null||port==null) {
		    	 throw new Exception("Update issue of parameters null.");
		     }
		     serversLoad.put(serverID, new Integer(serverLoad.intValue()));
		     serversAddr.put(serverID, new String[] {hostname,Integer.toString(port.intValue())});
		
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
//				log.debug("doing activity");
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
