package edu.stevens.cs549.dhts.resource;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.media.sse.EventOutput;

import edu.stevens.cs549.dhts.activity.DHT;
import edu.stevens.cs549.dhts.activity.DHTBase.Failed;
import edu.stevens.cs549.dhts.activity.DHTBase.Invalid;
import edu.stevens.cs549.dhts.activity.IDHTResource;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.main.Log;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 * 
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 * 
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService {

	private static final String TAG = NodeService.class.getCanonicalName();

	private static Logger logger = Logger.getLogger(TAG);

	// TODO: add the missing operations

	HttpHeaders headers;

	IDHTResource dht;

	private void error(String mesg, Exception e) {
		logger.log(Level.SEVERE, mesg, e);
	}

	public NodeService(HttpHeaders headers, UriInfo uri) {
		this.headers = headers;
		this.dht = new DHT(uri);
	}

	private Response response(NodeInfo n) {
		return Response.ok(n).build();
	}

	// FIXME added in method
	private Response response(String[] s) {
		return Response.ok(s).build();
	}

	private Response response(TableRep t) {
		return Response.ok(t).build();
	}

	private Response response(TableRow r) {
		return Response.ok(r).build();
	}

	private Response responseNull() {
		return Response.notModified().build();
	}

	private Response response(EventOutput e) {
		return Response.ok(e).build();
	}

	private Response response() {
		return Response.ok().build();
	}

	public Response getNodeInfo() {
		Log.weblog(TAG, "getNodeInfo()");
		return response(dht.getNodeInfo());
	}

	public Response getPred() {
		Log.weblog(TAG, "getPred()");
		return response(dht.getPred());
	}

// FIXME added in GETSUCC
	public Response getSucc() {
		Log.weblog(TAG, "getSucc()");
		return response(dht.getSucc());
	}

	// FIXME added in CLOSESTPRECEDINGFINGER
	public Response closestPrecedingFinger(int id) {
		Log.weblog(TAG, "closestPrecedingFinger");
		return response(dht.closestPrecedingFinger(id));
	}

	public Response notify(TableRep predDb) {
		Log.weblog(TAG, "notify()");
		TableRep db = dht.notify(predDb);
		if (db == null) {
			return responseNull();
		} else {
			return response(db);
		}
	}

	// FIXME added in GETBINDING
	public Response getBinding(String key) {
		Log.weblog(TAG, "getBinding");
		try {
			String[] result = dht.get(key);
			return response(result);
		} catch (Exception e) {
			throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
		}
	}

	// FIXME need to add ADDBINDING
	public Response addBinding(String key, String value) {
		Log.weblog(TAG, "addBinding");
		try {
			dht.add(key, value);
			return response();
		} catch (Invalid e) {
			error("add", e);
			e.printStackTrace();
		}
		throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
	}

	// FIXME added DELETEBINDING
	public Response deleteBinding(String skey, String value) {
		Log.weblog(TAG, "deleteBinding()");
		try {
			dht.delete(skey, value);
			return response();
		} catch (Exception e) {
			System.out.println(e);
		}
		throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
	}

	public Response findSuccessor(int id) {
		try {
			Log.weblog(TAG, "findSuccessor()");
			// return response(dht.findSuccessor(id)); FIXME edited this method
			// to how it looked on old assignment
			dht.findSuccessor(id); // added from the previous assignment
			return response(); // added from the previous assignment
		} catch (Failed e) {
			error("findSuccessor", e);
			throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
		}
	}
	
	public Response stopListening(int id, String key) {
		Log.weblog(TAG, "stopListening()");
		dht.stopListening(id, key);
		return response();
	}
	
	public Response listenForBindings(int id, String key) {
		Log.weblog(TAG, "listenForBindings()");
		return response(dht.listenForBindings(id, key));
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}