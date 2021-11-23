package edu.stevens.cs549.dhts.main;

import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.sse.EventSource;
import org.glassfish.jersey.media.sse.SseFeature;

import edu.stevens.cs549.dhts.activity.DHTBase;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.resource.TableRep;

public class WebClient {

	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	/*
	 * Encapsulate Web client operations here.
	 * 
	 * TODO: Fill in missing operations.
	 */

	/*
	 * Creation of client instances is expensive, so just create one.
	 */
	protected Client client;
	
	protected Client listenClient;

	public WebClient() {
		client = ClientBuilder.newBuilder()
				.register(ObjectMapperProvider.class)
				.register(JacksonFeature.class)
				.build();
		listenClient = ClientBuilder.newBuilder()
				.register(ObjectMapperProvider.class)
				.register(JacksonFeature.class)
				.register(SseFeature.class).build();	
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	private Response getRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request() //FIXME took out MediaType.APPLICATION_JSON_TYPE to match old code might have to put back
					.get();
			return cr;
		} catch (Exception e) {
			error("Exception during GET request", e);
			return null;
		}
	}

	private Response putRequest(URI uri, TableRep tableRep) {
		// TODO Complete. *
		try { 
			Response cr = client.target(uri) 
			.request(MediaType.APPLICATION_JSON_TYPE)
			.put(Entity.entity(tableRep, MediaType.APPLICATION_JSON_TYPE));
			return cr; 
		} catch (Exception e) { 
			error("Exception during PUT request", e); 
	
		return null;
	}
}
	
	private Response putRequest(URI uri) {
		try {
			Response cr = client.target(uri)
					.request()
					.put(Entity.text(""));
			return cr;
		} catch (Exception e) {
			error("Exception during PUT request", e);
			return null;
		}
	}

	//FIXME added delete method in myself not professors code
		private Response deleteRequest (URI uri) { 
			try { 
				Response cr = client.target(uri)
						.request()
						.delete();  
				return cr; 
			} catch (Exception e) { 
				error ("Exception during DELETE request", e); 
				return null; 
			}
		}
	
		//added method for assignement 3 may need to take out if not working  							       	FIXME
		private Response stopListenRequest(URI uri) {
			try {
				Response cr = client.target(uri)
						.request(MediaType.APPLICATION_JSON_TYPE)
						.delete();
				return cr;
			} catch(Exception e) {
				System.out.println(e);
				return null;
			}
		}
		
	/*
	 * Ping a remote site to see if it is still available.
	 */
	public boolean isFailed(URI base) {
		URI uri = UriBuilder.fromUri(base).path("info").build();
		Response c = getRequest(uri);
		return c.getStatus() >= 300;
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getPred(NodeInfo node) throws DHTBase.Failed {
		URI predPath = UriBuilder.fromUri(node.addr).path("pred").build();
		info("client getPred(" + predPath + ")");
		Response response = getRequest(predPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /pred");
		} else {
			NodeInfo pred = response.readEntity(NodeInfo.class);
			return pred;
		}
	}

// FIXME added the below method in myself not the professors code 
	
	public NodeInfo getSucc(NodeInfo node) throws DHTBase.Failed { 
		URI succPath = UriBuilder.fromUri(node.addr).path("succ").build(); 
		info("client getSucc(" + succPath + ")"); 
		Response response = getRequest(succPath); 
		if (response == null || response.getStatus() >= 300) { 
			throw new DHTBase.Failed("Get /succ"); 
		} else { 
			NodeInfo succ = response.readEntity(NodeInfo.class); 
			return succ; 
		}
	}
	
//FIXME added the below method in myself not the professors code but in rubric 
	
		public NodeInfo getClosestPrecedingFinger (NodeInfo node, int id) throws DHTBase.Failed { 
		//	UriBuilder ub = UriBuilder.fromUri(node.addr).path("finger"); 
		//	URI closestPrec = ub.queryParam("id", id).build(); 
			UriBuilder ub = UriBuilder.fromUri(node.addr); 
			URI closestPrec = ub.path("finger").queryParam("id", id).build(); 
			info("client closestPrecedingFinger("+ closestPrec + ")"); 
			Response response = getRequest(closestPrec); 
			if (response == null || response.getStatus() >= 300) { 
				throw new DHTBase.Failed("Get /finger?id=ID"); 
			} else { 
				NodeInfo fingerVal = response.readEntity(NodeInfo.class); 
				return fingerVal; 
			}
		}	
	
	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public TableRep notify(NodeInfo node, TableRep predDb) throws DHTBase.Failed {
		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
		NodeInfo thisNode = predDb.getInfo();
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("notify");
		URI notifyPath = ub.queryParam("id", thisNode.id).build();
		info("client notify(" + notifyPath + ")");
		Response response = putRequest(notifyPath, predDb);
		if (response != null && response.getStatusInfo() == Response.Status.NOT_MODIFIED) {
			/*
			 * Do nothing, the successor did not accept us as its predecessor.
			 */
			return null;
		} else if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("PUT /notify?id=ID");
		} else {
			TableRep bindings = response.readEntity(TableRep.class);
			return bindings;
		}
	}
	
	// TODO * methods added from previous assignment lines 198 to 261
	/*
	 * Get bindings under a key.
	 */
	public String[] get(NodeInfo node, String skey) throws DHTBase.Failed {
		//throw new IllegalStateException("Unimplemented get");
		UriBuilder ub = UriBuilder.fromUri(node.addr); 
		URI getKeyValuePath = ub.queryParam("key", skey).build(); 
		info("client getKeyValuePath(" + getKeyValuePath + ")"); 
		Response response = getRequest(getKeyValuePath); 
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("Get ?key=KEY"); 
		}else { 
			String [] result = response.readEntity(String[].class); 
			return result; 
		}
	}

	// TODO *
	/*
	 * Put bindings under a key.
	 */
	public void add(NodeInfo node, String skey, String v) throws DHTBase.Failed {
		//throw new IllegalStateException("Unimplemented add");
		UriBuilder ub = UriBuilder.fromUri(node.addr); 
		URI addPath = ub.queryParam("key", skey).queryParam("value", v).build(); 
		info("client add (" + addPath + ")"); 
		Response response = putRequest(addPath); 
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("Put ?key=Key & val=VAL"); 
		}
	}

	// TODO *
	/*
	 * Delete bindings under a key.
	 */
	public void delete(NodeInfo node, String skey, String v) throws DHTBase.Failed {
		//throw new IllegalStateException("Unimplemented delete");
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("delete"); 
		URI deletePath = ub.queryParam("key", skey).queryParam("value", v).build(); 
		info("client delete (" + deletePath + ")");
		Response response = deleteRequest(deletePath); 
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("Delete ?key=Key & val=VAL"); 
		}
	}

	
	
	// TODO *
	/*
	 * Find successor of an id. Used by join protocol
	 */
	public NodeInfo findSuccessor(URI addr, int id) throws DHTBase.Failed {
		//throw new IllegalStateException("Unimplemented findSuccessor");
		UriBuilder ub = UriBuilder.fromUri(addr).path("succ"); //may need to change to find
		URI findPath = ub.queryParam("id", id).build(); 
		info("client findSuccessor (" + findPath + ")");
		Response response = getRequest(findPath);
		if (response == null || response.getStatus() >= 300) {
			throw new DHTBase.Failed("GET /find?id=ID"); 
	}else { 
	return response.readEntity(NodeInfo.class); 
	}
}

	/*
	 * Operations for listening for new bindings.
	 */
	public EventSource listenForBindings(NodeInfo node, int id, String skey) throws DHTBase.Failed {
		// TODO listen for SSE subscription requests on http://.../dht/listen?key=<key>
		// On the service side, don't expect LT request or response headers for this request.
		// Note: "id" is client's id, to enable us to stop event generation at the server.
		// May have to fix code lines 272 to 278                                                              FIXME
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("listen"); 
		URI listenForBs = ub.queryParam("id", id).queryParam("key", skey).build(); 
		info("client listen for bindings path " + listenForBs); 
		
		WebTarget target = listenClient.target(listenForBs); 
		
		return EventSource.target(target).build();
	}

	public void listenOff(NodeInfo node, int id, String skey) throws DHTBase.Failed {
		// TODO listen for SSE subscription requests on http://.../dht/listen?key=<key>
		// may have to fix code 																		       FIXME
		UriBuilder ub = UriBuilder.fromUri(node.addr).path("listen"); 
		URI listenOffPath = ub.queryParam("id", id).queryParam("key", skey).build(); 		
		
		info("client listenOff "+listenOffPath);
		
		Response response = stopListenRequest(listenOffPath); 
		if(response == null || response.getStatus() >= 300) { 
			throw new DHTBase.Failed("DELETE/listen?key=Key&val=VAL");
		}
		
	}

}































