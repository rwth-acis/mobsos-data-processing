package i5.las2peer.services.mobsos.dataProcessing;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import i5.las2peer.api.Context;
import i5.las2peer.api.ManualDeployment;
import i5.las2peer.api.Service;
import i5.las2peer.api.execution.InternalServiceException;
import i5.las2peer.api.execution.ServiceAccessDeniedException;
import i5.las2peer.api.execution.ServiceInvocationFailedException;
import i5.las2peer.api.execution.ServiceMethodNotFoundException;
import i5.las2peer.api.execution.ServiceNotAuthorizedException;
import i5.las2peer.api.execution.ServiceNotAvailableException;
import i5.las2peer.api.execution.ServiceNotFoundException;
import i5.las2peer.api.logging.MonitoringEvent;
import i5.las2peer.api.security.Agent;
import i5.las2peer.api.security.AgentException;
import i5.las2peer.logging.bot.BotMessage;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.security.BotAgent;
import i5.las2peer.security.MonitoringAgent;
import i5.las2peer.security.ServiceAgentImpl;
import i5.las2peer.services.mobsos.dataProcessing.database.DatabaseInsertStatement;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabase;
import i5.las2peer.services.mobsos.dataProcessing.database.SQLDatabaseType;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import net.minidev.json.parser.ParseException;

/**
 * 
 * This service is responsible for processing incoming monitoring data. It tests the data for correctness and stores
 * them in a relational database. The provision is done by the Monitoring Data Provision Service.
 * 
 */
@ManualDeployment
public class MonitoringDataProcessingService extends Service {
	private static final String AGENT_PASS = "ProcessingAgentPass"; // The pass phrase for the receivingAgent
	private MonitoringAgent receivingAgent; // This agent will be responsible for receiving all incoming message
	private Map<String, String> monitoredServices = new HashMap<String, String>(); // A list of services that are
																					// monitored
	private BotAgent actingAgent;
	private Set<String> triggerFunctions = new HashSet<String>();
	private ArrayList<BotMessage> botMessages = new ArrayList<BotMessage>();

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
	private Connection con;
	private SQLDatabase database; // The database instance to write to.

	private int messageCount;

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
		try {
			con = database.getDataSource().getConnection();
		} catch (SQLException e) {
			System.out.println("Failed to Connect");
		}
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
		Agent requestingAgent = Context.getCurrent().getMainAgent();
		if (receivingAgent == null) {
			System.out.println("Monitoring: Agent not registered yet, this invocation must be false!");
			return false;
		}
		if (!requestingAgent.getIdentifier().equals(receivingAgent.getIdentifier())) {
			System.out.println("Monitoring: I only take messages from my own agent!");
			return false;
		}
		messageCount = 0;
		for (int i = 0; i < messages.length; ++i) {
			if (messages[i] != null)
				messageCount++;
		}
		System.out.println("Monitoring: Got " + messageCount + " monitoring messages!");
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
		botMessages = new ArrayList<BotMessage>();
		for (MonitoringMessage message : messages) {
			// Happens when a node has sent its last messages
			if (message == null) {
				counter++;
			} else if (message.getEvent() == MonitoringEvent.BOT_ADD_TO_MONITORING) {
				try {
					JSONObject jr = new JSONObject();
					JSONParser p = new JSONParser(JSONParser.MODE_PERMISSIVE);

					jr = (JSONObject) p.parse(message.getRemarks());
					ServiceAgentImpl sa = (ServiceAgentImpl) Context.get().getServiceAgent();
					try {
						actingAgent = (BotAgent) sa.getRunningAtNode().getAgent(jr.getAsString("botId"));
					} catch (AgentException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					JSONArray jra = (JSONArray) jr.get("triggerFunctions");
					for (int i = 0; i < jra.size(); i++) {
						triggerFunctions.add(((String) jra.get(i)).toLowerCase());
					}
					System.out.println("\u001B[32mBot " + actingAgent.getLoginName() + " added.\u001B[0m");
					returnStatement = persistMessage(message, "MESSAGE");
					if (!returnStatement)
						counter++;
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				// Add node to database (running means we got an id representation)
			} else if ((message.getEvent() == MonitoringEvent.NODE_STATUS_CHANGE
					&& message.getRemarks().equals("RUNNING"))) {
				returnStatement = persistMessage(message, "NODE");
				if (!returnStatement)
					counter++;

				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			}

			// Add unregister date to all registered agents at this node
			else if (message.getEvent() == MonitoringEvent.NODE_STATUS_CHANGE
					&& message.getRemarks().equals("CLOSING")) {
				returnStatement = persistMessage(message, "REGISTERED_AT");
				if (!returnStatement)
					counter++;
				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
			}

			// Add service to monitored service list and add service, service agent, 'registered at' and message to
			// database
			else if (message.getEvent() == MonitoringEvent.SERVICE_ADD_TO_MONITORING) {
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
			else if (message.getEvent() == MonitoringEvent.AGENT_REGISTERED
					&& !message.getRemarks().equals("ServiceAgent")
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
			else if (message.getEvent() == MonitoringEvent.HTTP_CONNECTOR_REQUEST) {
				if (message.getSourceAgentId() == null || monitoredServices.containsKey(message.getSourceAgentId())) {
					returnStatement = persistMessage(message, "MESSAGE");
					if (!returnStatement)
						counter++;
				}
			}

			// If enabled for monitoring, add service message to database
			else if (Math.abs(message.getEvent().getCode()) >= 7000
					&& (Math.abs(message.getEvent().getCode()) < 8000)) {
				if (message.getEvent() == MonitoringEvent.SERVICE_SHUTDOWN) {
					returnStatement = persistMessage(message, "REGISTERED_AT");
					if (!returnStatement)
						counter++;
				}
				returnStatement = persistMessage(message, "MESSAGE");
				if (!returnStatement)
					counter++;
				JSONParser p = new JSONParser(JSONParser.MODE_PERMISSIVE);
				try {
					Object jo = p.parse(message.getRemarks());
					if (jo instanceof JSONObject) {
						String function = ((JSONObject) jo).getAsString("functionName");
						if (function != null && hasBot() && triggerFunctions.contains(function.toLowerCase())) {
							BotMessage m = new BotMessage(message.getTimestamp(), message.getEvent(),
									message.getSourceNode(), message.getSourceAgentId(), message.getDestinationNode(),
									message.getDestinationAgentId(), message.getRemarks());
							botMessages.add(m);
						}
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if (message.getEvent() == MonitoringEvent.AGENT_REMOVED) {
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
		if (!botMessages.isEmpty()) {
			try {
				Context.getCurrent().invoke("i5.las2peer.services.socialBotManagerService.SocialBotManagerService",
						"getMessages", (Serializable) botMessages);
				// actingAgent.receiveMessage(m, actingAgent.getRunningAtNode().getAgentContext(actingAgent));
			} catch (ServiceNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ServiceNotAvailableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InternalServiceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ServiceMethodNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ServiceInvocationFailedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ServiceAccessDeniedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ServiceNotAuthorizedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println((messages.length - counter) + "/" + messageCount + " messages were handled.");
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
		if (con == null) {
			return false;
		}
		try {
			PreparedStatement insertStatement = DatabaseInsertStatement.returnInsertStatement(con, message,
					database.getJdbcInfo(), DB2Schema, table, hashRemarks);
			int result = insertStatement.executeUpdate();
			if (result >= 0) {
				returnStatement = true;
			}
			insertStatement.close();
		} catch (Exception e) {
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
	public String getReceivingAgentId(String greetings) {
		System.out.println("Monitoring: Service requests receiving agent id: " + greetings);
		if (receivingAgent == null) {
			try {
				receivingAgent = MonitoringAgent.createMonitoringAgent(AGENT_PASS);
				receivingAgent.unlock(AGENT_PASS);
				Context.getCurrent().storeAgent(receivingAgent);
				Context.getCurrent().registerReceiver(receivingAgent);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.receivingAgent.getIdentifier();
	}

	public boolean hasBot() {
		return actingAgent != null;
	}
}
