package edu.stevens.cs549.dhts.resource;

import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.media.sse.SseFeature;

import edu.stevens.cs549.dhts.activity.DHTBase.Invalid;

@Path("/dht")
public class NodeResource {

	/*
	 * Web service API.
	 * 
	 * TODO: Fill in the missing operations.
	 */

	Logger log = Logger.getLogger(NodeResource.class.getCanonicalName());

	@Context
	UriInfo uriInfo;

	@Context
	HttpHeaders headers;

	@GET
	@Path("info")
	@Produces("application/json")
	public Response getNodeInfo() {
		return new NodeService(headers, uriInfo).getNodeInfo();
	}

	@GET
	@Path("pred")
	@Produces("application/json")
	public Response getPred() {
		return new NodeService(headers, uriInfo).getPred();
	}

	@PUT
	@Path("notify")
	@Consumes("application/json")
	@Produces("application/json")
	/*
	 * Actually returns a TableRep
	 */
	public Response putNotify(TableRep predDb) {
		/*
		 * See the comment for WebClient::notify (the client side of this logic).
		 */
		return new NodeService(headers, uriInfo).notify(predDb);
	}

	@GET
	@Path("find")//
	@Produces("application/json")
	public Response findSuccessor(@QueryParam("id") String index) {
		int id = Integer.parseInt(index);
		return new NodeService(headers, uriInfo).findSuccessor(id);
	}
	
	//FIXME added GETBINDING method
		@GET
		@Path("")
		@Produces("application/json")
		public Response getBinding(@QueryParam("key") String key) { 
			return new NodeService(headers, uriInfo).getBinding(key); 
		}
		
		//FIXME added in PUTBINDING method
		@PUT
		@Path("")
		@Produces("application/json")
		public Response putBinding(@QueryParam("key") String key, @QueryParam ("value") String value){ 
			return new NodeService(headers, uriInfo).addBinding(key, value); 
		}
		
		//FIXME added in DELETEBINDING method
		@DELETE
		@Path("delete")  //FIXME put in delete when the path was empty *****
		@Consumes("application/json")
		@Produces("application/json") //FIXME had this commented out now put back in ****
		public Response deleteBinding(@QueryParam("key")String key, @QueryParam ("value") String value) { 
			return new NodeService(headers, uriInfo).deleteBinding(key, value); 
		}
		
		//FIXME added in GETSUCC method
		@GET
		@Path("succ")
		@Produces("application/json")
		public Response getSucc() { 
			return new NodeService(headers, uriInfo).getSucc(); 
		}
		
		//FIXME added in CLOSESTPRECEDINGFINGER method
		@GET
		@Path("finger")
		@Produces("application/json")
		public Response getFinger(@QueryParam("id") String index) { 
			int id = Integer.parseInt(index); 
			return new NodeService(headers, uriInfo).closestPrecedingFinger(id); 
			}
		
		//testing new code from here down above this line is from old assignment
		
		// may have to take out code from 112 to 141   FIXME
		
		@DELETE
		@Path("listen")
		public Response listenOff(@QueryParam("id") int id, @QueryParam("key") String key) {
			return new NodeService(headers, uriInfo).stopListening(id, key);
		}
		
		@GET
		@Path("listen")
		@Produces(SseFeature.SERVER_SENT_EVENTS)
		public Response listenOn(@QueryParam("id") int id, @QueryParam("key") String key) {
			return new NodeService(headers,uriInfo).listenForBindings(id,key);
		}
}
