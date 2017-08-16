package i5.las2peer.services.mobsos.dataProcessing;

import static org.junit.Assert.assertEquals;
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
import i5.las2peer.security.MonitoringAgent;
import i5.las2peer.security.ServiceAgent;
import i5.las2peer.security.UserAgent;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabaseType;
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
			SQLDatabaseType t0 = SQLDatabaseType.getSQLDatabaseType(0);
			assert (t0.getDriverName() == null);
			assert (t0.getURLPrefix("", "", 0) == null);
			SQLDatabaseType t1 = SQLDatabaseType.getSQLDatabaseType(1);
			assertEquals(t1.getDriverName(), "com.ibm.db2.jcc.DB2Driver");
			assertEquals(t1.getURLPrefix("", "", 0), "jdbc:db2://:0/");
			MonitoringMessage m1 = new MonitoringMessage((long) 1376750476, Event.NODE_STATUS_CHANGE, "1234567891011",
					(long) 1, "1234567891022", (long) 2, "{}");
			MonitoringMessage m2 = new MonitoringMessage((long) 1376750476, Event.NODE_STATUS_CHANGE, "1234567891011",
					(long) 1, "1234567891022", (long) 2, "{\"msg\": \"RUNNING\"}");
			MonitoringMessage m3 = new MonitoringMessage((long) 1376750476, Event.NODE_STATUS_CHANGE, "1234567891011",
					(long) 1, "1234567891022", (long) 2, "{\"msg\": \"CLOSING\"}");
			MonitoringMessage m4 = new MonitoringMessage((long) 1376750476, Event.SERVICE_ADD_TO_MONITORING,
					"1234567891011", (long) 1, "1234567891022", (long) 2, "{}");
			MonitoringMessage m5 = new MonitoringMessage((long) 1376750476, Event.AGENT_REGISTERED, "1234567891011",
					(long) 1, "1234567891022", (long) 2, "{\"msg\": \"UserAgent\"}");
			MonitoringMessage m6 = new MonitoringMessage((long) 1376750476, Event.HTTP_CONNECTOR_REQUEST,
					"1234567891011", (long) 1, "1234567891022", (long) 2, "{}");
			MonitoringMessage m7 = new MonitoringMessage((long) 1376750476, Event.SERVICE_CUSTOM_MESSAGE_1,
					"1234567891011", (long) 1, "1234567891022", (long) 2, "{}");
			MonitoringMessage m8 = new MonitoringMessage((long) 1376750476, Event.SERVICE_SHUTDOWN, "1234567891011",
					(long) 1, "1234567891022", (long) 2, "{}");
			MonitoringMessage m9 = new MonitoringMessage((long) 1376750476, Event.AGENT_REMOVED, "1234567891011",
					(long) 1, "1234567891022", (long) 2, "{}");
			MonitoringMessage[] m = { m1, m2, m3, m4, m5, m6, m7, m8, m9 };

			Object result2 = node.invoke(testService, testServiceClass, "getMessages", new Serializable[] { m });
			assertTrue(result2 instanceof Boolean);
			assert ((Boolean) result2 == false);

			Object result = node.invoke(testService, testServiceClass, "getReceivingAgentId",
					new Serializable[] { "Test message." });
			assertTrue(result instanceof Long);
			result = node.invoke(testService, testServiceClass, "getReceivingAgentId",
					new Serializable[] { "Test message2." });
			assertTrue(result instanceof Long);
			MonitoringAgent mAgent = (MonitoringAgent) node.getAgent((Long) result);
			mAgent.unlockPrivateKey("ProcessingAgentPass");

			result2 = node.invoke(testService, testServiceClass, "getMessages", new Serializable[] { m });
			assertTrue(result2 instanceof Boolean);
			assert ((Boolean) result2 == false);

			result2 = node.invoke(mAgent, testServiceClass, "getMessages", new Serializable[] { m });
			assertTrue(result2 instanceof Boolean);
			MonitoringMessage[] mm = new MonitoringMessage[2];
			Object result3 = node.invoke(mAgent, testServiceClass, "getMessages", new Serializable[] { mm });
			assertTrue(result3 instanceof Boolean);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception: " + e);
		}
	}

}
