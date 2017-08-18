package i5.las2peer.services.mobsos.dataProcessing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import i5.las2peer.api.logging.MonitoringEvent;
import i5.las2peer.api.p2p.ServiceNameVersion;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.p2p.PastryNodeImpl;
import i5.las2peer.persistency.SharedStorage.STORAGE_MODE;
import i5.las2peer.security.MonitoringAgent;
import i5.las2peer.security.ServiceAgentImpl;
import i5.las2peer.security.UserAgentImpl;
import i5.las2peer.services.mobsos.dataProcessing.database.DatabaseInsertStatement;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabase;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabaseType;
import i5.las2peer.testing.MockAgentFactory;
import i5.las2peer.testing.TestSuite;

public class MonitoringDataProcessingServiceTest {

	private PastryNodeImpl node;
	private UserAgentImpl adam = null;
	private ServiceAgentImpl testService = null;
	private static SQLDatabase database;
	Properties prop;
	private static int dbType;
	private static String dbUser;
	private static String dbPass;
	private static String dbName;
	private static int dbPort;
	private static String dbHost;

	private static final String adamsPass = "adamspass";
	private static final ServiceNameVersion testServiceClass = new ServiceNameVersion(
			MonitoringDataProcessingService.class.getCanonicalName(), "0.1");

	private final static String sNode = "1234567891011";
	private final static String dNode = "1234567891022";
	private final static String sAgent = "c4ca4238a0b923820dcc509a6f75849b"; // md5 for 1

	@BeforeClass
	public static void setUpDatabase() {
		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream(
					"etc/i5.las2peer.services.mobsos.dataProcessing.MonitoringDataProcessingService.properties");

			// load a properties file
			prop.load(input);
			dbType = Integer.parseInt(prop.getProperty("databaseTypeInt"));
			dbUser = prop.getProperty("databaseUser");
			dbPass = prop.getProperty("databasePassword");
			dbName = prop.getProperty("databaseName");
			dbPort = Integer.parseInt(prop.getProperty("databasePort"));
			dbHost = prop.getProperty("databaseHost");
		} catch (IOException ex) {
			System.out.println(ex.getMessage());
			ex.printStackTrace();
			fail();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

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

	@AfterClass
	public static void cleanDB() {
		System.out.println("clean db...");
		if (database != null) {
			Connection c;
			try {
				c = database.getDataSource().getConnection();
				Statement s = c.createStatement();
				s.executeUpdate("DELETE FROM MESSAGE WHERE SOURCE_AGENT='" + sAgent + "'");
				s.executeUpdate("DELETE FROM SERVICE WHERE AGENT_ID='" + sAgent + "'");
				s.executeUpdate("DELETE FROM REGISTERED_AT WHERE AGENT_ID='" + sAgent + "'");
				s.executeUpdate("DELETE FROM AGENT WHERE AGENT_ID='" + sAgent + "'");
				s.executeUpdate("DELETE FROM NODE WHERE NODE_LOCATION='" + sNode + "'");
				s.close();
				c.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

	@Test
	public void testDefaultStartup() {
		try {
			// codecoverage
			new DatabaseInsertStatement();
			MonitoringMessage ccm = new MonitoringMessage((long) 1376750476, MonitoringEvent.SERVICE_CUSTOM_MESSAGE_1,
					sNode, null, dNode, null, "1337");
			// TODO
			new MonitoringMessageWithEncryptedAgents(ccm, false);

			// unknown type
			SQLDatabaseType t0 = SQLDatabaseType.getSQLDatabaseType(0);
			assert (t0 == null);
			// db2 workaround
			SQLDatabaseType t1 = SQLDatabaseType.getSQLDatabaseType(1);
			assertEquals(t1.getDriverName(), "com.ibm.db2.jcc.DB2Driver");
			assertEquals(t1.getURLPrefix("", "", 0), "jdbc:db2://:0/");

			// mysql test
			SQLDatabaseType t2 = SQLDatabaseType.getSQLDatabaseType(dbType);
			database = new SQLDatabase(t2, dbUser, dbPass, dbName, dbHost, dbPort);
			try {
				Connection c = database.getDataSource().getConnection();
				assertEquals(database.getUser(), dbUser);
				assertEquals(database.getPassword(), dbPass);
				assertEquals(database.getDatabase(), dbName);
				int port = database.getPort();
				assert (port == dbPort);
				assertEquals(database.getHost(), dbHost);
				c.close();
			} catch (SQLException e) {
				e.printStackTrace();
				fail();
			}

			// Monitoring messages
			MonitoringMessage m1 = new MonitoringMessage((long) 1376750476, MonitoringEvent.NODE_STATUS_CHANGE, sNode,
					"1", dNode, "2", "{}");
			MonitoringMessage m2 = new MonitoringMessage((long) 1376750476, MonitoringEvent.NODE_STATUS_CHANGE, sNode,
					"1", dNode, "2", "{\"msg\": \"RUNNING\"}");
			MonitoringMessage m3 = new MonitoringMessage((long) 1376750476, MonitoringEvent.NODE_STATUS_CHANGE, sNode,
					"1", dNode, "2", "{\"msg\": \"CLOSING\"}");
			MonitoringMessage m4 = new MonitoringMessage((long) 1376750476, MonitoringEvent.SERVICE_ADD_TO_MONITORING,
					sNode, "1", dNode, "2", "{}");
			MonitoringMessage m5 = new MonitoringMessage((long) 1376750476, MonitoringEvent.AGENT_REGISTERED, sNode,
					"1", dNode, "2", "{\"msg\": \"UserAgent\"}");
			MonitoringMessage m6 = new MonitoringMessage((long) 1376750476, MonitoringEvent.HTTP_CONNECTOR_REQUEST,
					sNode, "1", dNode, "2", "{}");
			MonitoringMessage m7 = new MonitoringMessage((long) 1376750476, MonitoringEvent.SERVICE_CUSTOM_MESSAGE_1,
					sNode, "1", dNode, "2", "{}");
			MonitoringMessage m8 = new MonitoringMessage((long) 1376750476, MonitoringEvent.SERVICE_SHUTDOWN, sNode,
					"1", dNode, "2", "{}");
			MonitoringMessage m9 = new MonitoringMessage((long) 1376750476, MonitoringEvent.AGENT_REMOVED, sNode, "1",
					dNode, "2", "{}");
			MonitoringMessage[] m = { m1, m2, m3, m4, m5, m6, m7 };

			// test with service agent, monitoring agents does not exist
			Object result2 = node.invoke(testService, testServiceClass, "getMessages", new Serializable[] { m });
			assertTrue(result2 instanceof Boolean);
			assert ((Boolean) result2 == false);

			// get monitoring agent (should create one)
			Object result = node.invoke(testService, testServiceClass, "getReceivingAgentId",
					new Serializable[] { "Test message." });
			assertTrue(result instanceof String);
			// get created agent
			result = node.invoke(testService, testServiceClass, "getReceivingAgentId",
					new Serializable[] { "Test message2." });
			assertTrue(result instanceof String);
			// fetch agent
			MonitoringAgent mAgent = (MonitoringAgent) node.getAgent((String) result);
			mAgent.unlock("ProcessingAgentPass");

			// try to get messages with service agent, monitoring agents exists
			result2 = node.invoke(testService, testServiceClass, "getMessages", new Serializable[] { m });
			assertTrue(result2 instanceof Boolean);
			assert ((Boolean) result2 == false);

			// try with monitoring agent
			result2 = node.invoke(mAgent, testServiceClass, "getMessages", new Serializable[] { m });
			assertTrue(result2 instanceof Boolean);

			// try with empty messages
			MonitoringMessage[] mm = new MonitoringMessage[2];
			Object result3 = node.invoke(mAgent, testServiceClass, "getMessages", new Serializable[] { mm });
			assertTrue(result3 instanceof Boolean);
			// now with messages
			mm[0] = m8;
			mm[1] = m9;
			result3 = node.invoke(mAgent, testServiceClass, "getMessages", new Serializable[] { mm });
			assertTrue(result3 instanceof Boolean);

			// remarks null or non json
			MonitoringMessage m10 = new MonitoringMessage((long) 1376750476, MonitoringEvent.SERVICE_CUSTOM_MESSAGE_2,
					sNode, "1", dNode, "2", "test");
			MonitoringMessage m11 = new MonitoringMessage((long) 1376750476, MonitoringEvent.SERVICE_CUSTOM_MESSAGE_2,
					sNode, "1", dNode, "2", null);
			MonitoringMessage[] mmm = { m10, m11 };
			Object result4 = node.invoke(mAgent, testServiceClass, "getMessages", new Serializable[] { mmm });
			assertTrue(result4 instanceof Boolean);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception: " + e);
		}
	}

}
