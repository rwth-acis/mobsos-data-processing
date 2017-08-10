package i5.las2peer.services.mobsos.dataProcessing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

import i5.las2peer.api.Service;
import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.security.Agent;
import i5.las2peer.security.AgentException;
import i5.las2peer.security.L2pSecurityException;
import i5.las2peer.security.MonitoringAgent;
import i5.las2peer.services.mobsos.dataProcessing.database.DatabaseInsertStatement;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabase;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabaseType;
import i5.las2peer.tools.CryptoException;

/**
 * 
 * This service is responsible for processing incoming monitoring data. It tests the data for correctness and stores
 * them in a relational database. The provision is done by the Monitoring Data Provision Service.
 * 
 */
public class MonitoringDataProcessingService extends Service {
	private static final String AGENT_PASS = "ProcessingAgentPass"; // The pass phrase for the receivingAgent
	private MonitoringAgent receivingAgent; // This agent will be responsible for receiving all incoming message
	private Map<Long, String> monitoredServices = new HashMap<Long, String>(); // A list of services that are monitored

	/**
	 * Configuration parameters, values will be set by the configuration file.
	 */
	private String databaseName;
	private int databaseTypeInt; // See SQLDatabaseType for more information
	private SQLDatabaseType databaseType;
	private String databaseHost;
	private int databasePort;
	private String databaseUser;
	private String databasePassword;
	private String DB2Schema; // Only needed if a DB2 database is used
	private boolean hashRemarks;

	private SQLDatabase database; // The database instance to write to.

	/**
	 * 
	 * Constructor of the Service. Loads the database values from a property file and tries to connect to the database.
	 * 
	 */
	public MonitoringDataProcessingService() {
		setFieldValues(); // This sets the values of the configuration file
		this.databaseType = SQLDatabaseType.getSQLDatabaseType(databaseTypeInt);
		this.database = new SQLDatabase(this.databaseType, this.databaseUser, this.databasePassword, this.databaseName,
				this.databaseHost, this.databasePort);
	}

	/**
	 * 
	 * Will be called by the receiving {@link i5.las2peer.security.MonitoringAgent} of this service, if it receives a
	 * message from a monitored node.
	 * 
	 * @param messages an array of {@link i5.las2peer.logging.monitoring.MonitoringMessage}s
	 * 
	 * @return true, if message persistence did work
	 * 
	 */
	public boolean getMessages(MonitoringMessage[] messages) {
		Agent requestingAgent = getContext().getMainAgent();
		if (receivingAgent == null) {
			System.out.println("Monitoring: Agent not registered yet, this invocation must be false!");
			return false;
		}
		if (requestingAgent.getId() != receivingAgent.getId()) {
			System.out.println("Monitoring: I only take messages from my own agent!");
			return false;
		}
		System.out.println("Monitoring: Got " + messages.length + " monitoring messages!");
		return processMessages(messages);
	}

	/**
	 * 
	 * Checks the messages content and calls {@link #persistMessage(MonitoringMessage, String)} with the corresponding
	 * values.
	 * 
	 * @param messages an array of {@link i5.las2peer.logging.monitoring.MonitoringMessage}s
	 * 
	 * @return true, if message persistence did work
	 * 
	 */
	private boolean processMessages(MonitoringMessage[] messages) {
		boolean returnStatement = true;
		int counter = 0;
		for (MonitoringMessage message : messages) {
			// Happens when a node has sent its last messages
			if (message == null) {
				counter++;
			}

			// Add node to database (running means we got an id representation)
			else if ((message.getEvent() == Event.NODE_STATUS_CHANGE && message.getRemarks().equals("RUNNING"))) {
				returnStatement = persistMessage(message, "NODE");
				if (!returnStatement)
					counter++;

				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			}

			// Add unregister date to all registered agents at this node
			else if (message.getEvent() == Event.NODE_STATUS_CHANGE && message.getRemarks().equals("CLOSING")) {
				returnStatement = persistMessage(message, "REGISTERED_AT");
				if (!returnStatement)
					counter++;
				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			}

			// Add service to monitored service list and add service, service agent, 'registered at' and message to
			// database
			else if (message.getEvent() == Event.SERVICE_ADD_TO_MONITORING) {
				monitoredServices.put(message.getSourceAgentId(), message.getRemarks());
				returnStatement = persistMessage(message, "AGENT");
				if (!returnStatement)
					counter++;

				returnStatement = persistMessage(message, "SERVICE");
				if (!returnStatement)
					counter++;

				returnStatement = persistMessage(message, "REGISTERED_AT");
				if (!returnStatement)
					counter++;

				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			}

			// Add agent to database
			else if (message.getEvent() == Event.AGENT_REGISTERED && !message.getRemarks().equals("ServiceAgent")
					&& !message.getRemarks().equals("ServiceInfoAgent")) {
				returnStatement = persistMessage(message, "AGENT");
				if (!returnStatement)
					counter++;

				returnStatement = persistMessage(message, "REGISTERED_AT");
				if (!returnStatement)
					counter++;

				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			}

			// Connector requests are only logged for monitored services or if they
			// do not give any information on the service itself
			else if (message.getEvent() == Event.HTTP_CONNECTOR_REQUEST) {
				if (message.getSourceAgentId() == null || monitoredServices.containsKey(message.getSourceAgentId())) {
					returnStatement = persistMessage(message, "MESSAGE");
					if (!returnStatement)
						counter++;
				}
			}

			// If enabled for monitoring, add service message to database
			else if (Math.abs(message.getEvent().getCode()) >= 7000
					&& (Math.abs(message.getEvent().getCode()) < 8000)) {
				if (message.getEvent() == Event.SERVICE_SHUTDOWN) {
					returnStatement = persistMessage(message, "REGISTERED_AT");
					if (!returnStatement)
						counter++;
				}
				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			} else if (message.getEvent() == Event.AGENT_REMOVED) {
				returnStatement = persistMessage(message, "REGISTERED_AT");
				if (!returnStatement)
					counter++;

				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			}
			// Just log the message
			else {
				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			}
		}
		System.out.println((messages.length - counter) + "/" + messages.length + " messages were handled.");
		return returnStatement;
	}

	/**
	 * 
	 * This method constructs SQL-statements by calling the {@link DatabaseInsertStatement} helper class. It then calls
	 * the database for persistence.
	 * 
	 * @param message a {@link i5.las2peer.logging.monitoring.MonitoringMessage}
	 * @param table the table to insert to. This parameter does determine what action will be performed on the database
	 *            (insert an agent, a message, a node..).
	 * 
	 * @return true, if message persistence did work
	 * 
	 */
	private boolean persistMessage(MonitoringMessage message, String table) {
		boolean returnStatement = false;
		try {
			Connection con = database.getDataSource().getConnection();
			PreparedStatement insertStatement = DatabaseInsertStatement.returnInsertStatement(con, message,
					database.getJdbcInfo(), DB2Schema, table, hashRemarks);
			int result = insertStatement.executeUpdate();
			if (result >= 0) {
				returnStatement = true;
			}
			insertStatement.close();
			con.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return returnStatement;
	}

	/**
	 * 
	 * Returns the id of this monitoring agent (that will be responsible for message receiving). Creates one if not
	 * existent.
	 * 
	 * @param greetings will be printed in the console and is only used to control registering
	 * 
	 * @return the id
	 * 
	 */
	public long getReceivingAgentId(String greetings) {
		System.out.println("Monitoring: Service requests receiving agent id: " + greetings);
		if (receivingAgent == null) {
			try {
				receivingAgent = MonitoringAgent.createMonitoringAgent(AGENT_PASS);
				receivingAgent.unlockPrivateKey(AGENT_PASS);
				getContext().getLocalNode().storeAgent(receivingAgent);
				getContext().getLocalNode().registerReceiver(receivingAgent);
			} catch (CryptoException | AgentException | L2pSecurityException e) {
				e.printStackTrace();
			}
		}
		return this.receivingAgent.getId();
	}
}
