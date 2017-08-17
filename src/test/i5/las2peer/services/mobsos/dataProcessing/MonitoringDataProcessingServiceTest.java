package i5.las2peer.services.mobsos.dataProcessing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import i5.las2peer.api.logging.MonitoringEvent;
import i5.las2peer.api.p2p.ServiceNameVersion;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.p2p.PastryNodeImpl;
import i5.las2peer.persistency.SharedStorage.STORAGE_MODE;
import i5.las2peer.security.MonitoringAgent;
import i5.las2peer.security.ServiceAgentImpl;
import i5.las2peer.security.UserAgentImpl;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabase;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabaseType;
import i5.las2peer.testing.MockAgentFactory;
import i5.las2peer.testing.TestSuite;

public class MonitoringDataProcessingServiceTest {

	private PastryNodeImpl node;
	private UserAgentImpl adam = null;
	private ServiceAgentImpl testService = null;

	private static final String adamsPass = "adamspass";
	private static final ServiceNameVersion testServiceClass = new ServiceNameVersion(
			MonitoringDataProcessingService.class.getCanonicalName(), "0.1");

	@Before
	public void startServer() throws Exception {
		// start Node
		node = TestSuite.launchNetwork(1, STORAGE_MODE.FILESYSTEM, true).get(0);

		adam = MockAgentFactory.getAdam();
		adam.unlock(adamsPass);
		node.storeAgent(adam);

		testService = ServiceAgentImpl.createServiceAgent(testServiceClass, "a pass");
		testService.unlock("a pass");

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
			assert (t0 == null);
			SQLDatabaseType t1 = SQLDatabaseType.getSQLDatabaseType(1);
			assertEquals(t1.getDriverName(), "com.ibm.db2.jcc.DB2Driver");
			assertEquals(t1.getURLPrefix("", "", 0), "jdbc:db2://:0/");
			SQLDatabaseType t2 = SQLDatabaseType.getSQLDatabaseType(2);
			SQLDatabase database = new SQLDatabase(t2, "root", "", "LAS2PEERMON", "127.0.0.1", 3306);
			try {
				database.getDataSource().getConnection();
			} catch (SQLException e) {
				e.printStackTrace();
				fail();
			}
			MonitoringMessage m1 = new MonitoringMessage((long) 1376750476, MonitoringEvent.NODE_STATUS_CHANGE,
					"1234567891011", "1", "1234567891022", "2", "{}");
			MonitoringMessage m2 = new MonitoringMessage((long) 1376750476, MonitoringEvent.NODE_STATUS_CHANGE,
					"1234567891011", "1", "1234567891022", "2", "{\"msg\": \"RUNNING\"}");
			MonitoringMessage m3 = new MonitoringMessage((long) 1376750476, MonitoringEvent.NODE_STATUS_CHANGE,
					"1234567891011", "1", "1234567891022", "2", "{\"msg\": \"CLOSING\"}");
			MonitoringMessage m4 = new MonitoringMessage((long) 1376750476, MonitoringEvent.SERVICE_ADD_TO_MONITORING,
					"1234567891011", "1", "1234567891022", "2", "{}");
			MonitoringMessage m5 = new MonitoringMessage((long) 1376750476, MonitoringEvent.AGENT_REGISTERED,
					"1234567891011", "1", "1234567891022", "2", "{\"msg\": \"UserAgent\"}");
			MonitoringMessage m6 = new MonitoringMessage((long) 1376750476, MonitoringEvent.HTTP_CONNECTOR_REQUEST,
					"1234567891011", "1", "1234567891022", "2", "{}");
			MonitoringMessage m7 = new MonitoringMessage((long) 1376750476, MonitoringEvent.SERVICE_CUSTOM_MESSAGE_1,
					"1234567891011", "1", "1234567891022", "2", "{}");
			MonitoringMessage[] m = { m1, m2, m3, m4, m5, m6, m7 };

			Object result2 = node.invoke(testService, testServiceClass, "getMessages", new Serializable[] { m });
			assertTrue(result2 instanceof Boolean);
			assert ((Boolean) result2 == false);

			Object result = node.invoke(testService, testServiceClass, "getReceivingAgentId",
					new Serializable[] { "Test message." });
			assertTrue(result instanceof String);
			result = node.invoke(testService, testServiceClass, "getReceivingAgentId",
					new Serializable[] { "Test message2." });
			assertTrue(result instanceof String);
			MonitoringAgent mAgent = (MonitoringAgent) node.getAgent((String) result);
			mAgent.unlock("ProcessingAgentPass");

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
