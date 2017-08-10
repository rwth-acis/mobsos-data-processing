package i5.las2peer.services.mobsos.dataProcessing;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.p2p.LocalNode;
import i5.las2peer.p2p.ServiceNameVersion;
import i5.las2peer.security.ServiceAgent;
import i5.las2peer.security.UserAgent;
import i5.las2peer.testing.MockAgentFactory;

public class MonitoringDataProcessingServiceTest {

	private LocalNode node;
	private UserAgent adam = null;
	private ServiceAgent testService = null;

	private static final String adamsPass = "adamspass";
	private static final ServiceNameVersion testServiceClass = new ServiceNameVersion(
			MonitoringDataProcessingService.class.getCanonicalName(), "0.1");

	@Before
	public void startServer() throws Exception {
		// start Node
		node = LocalNode.newNode();

		adam = MockAgentFactory.getAdam();
		adam.unlockPrivateKey(adamsPass);
		node.storeAgent(adam);

		node.launch();

		testService = ServiceAgent.createServiceAgent(testServiceClass, "a pass");
		testService.unlockPrivateKey("a pass");

		node.registerReceiver(testService);
	}

	@After
	public void stopNetwork() {
		try {
			System.out.println("stopping test network...");
			node.shutDown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testDefaultStartup() {
		try {

			Object result = node.invoke(testService, new ServiceNameVersion(testServiceClass.getName(), "0.1"),
					"getReceivingAgentId", new Serializable[] { "Test message." });
			assertTrue(result instanceof Long);

			MonitoringMessage[] m = {
					new MonitoringMessage(null, Event.NODE_STATUS_CHANGE, "1", (long) 1, "2", (long) 2, "{}") };
			Object result2 = node.invoke(testService, new ServiceNameVersion(testServiceClass.getName(), "0.1"),
					"getMessages", new Serializable[] { m });
			assertTrue(result2 instanceof Boolean);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception: " + e);
		}
	}

}
