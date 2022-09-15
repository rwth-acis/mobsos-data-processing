package i5.las2peer.services.mobsos.dataProcessing;

import i5.las2peer.restMapper.RESTService;
import i5.las2peer.restMapper.annotations.ServicePath;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

/**
 * Helper service used to test whether webhook calls are executed.
 */
@ServicePath("/webhooktestservice")
public class WebhookTestService extends RESTService {

    public static boolean webhookDelivered = false;

    @POST
    @Path("/webhook")
    public Response webhook(String body) {
        webhookDelivered = true;
        return Response.status(200).build();
    }
}
