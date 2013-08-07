package i5.las2peer.services.monitoring.processing;

import i5.las2peer.api.Service;
import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.security.Agent;
import i5.las2peer.security.AgentException;
import i5.las2peer.security.L2pSecurityException;
import i5.las2peer.security.MonitoringAgent;
import i5.las2peer.services.monitoring.processing.database.DatabaseInsertStatement;
import i5.las2peer.services.monitoring.processing.database.SQLDatabase;
import i5.las2peer.services.monitoring.processing.database.SQLDatabaseType;
import i5.las2peer.tools.CryptoException;

import java.util.HashMap;
import java.util.Map;


/**
 * 
 * MonitoringDataProcessingService.java
 * <br>
 * This service is responsible for processing incoming monitoring data.
 * It tests the data for correctness and stores them in a relational database.
 * The provision will be done by another service.
 * 
 * @author Peter de Lange
 * 
 */
public class MonitoringDataProcessingService extends Service{
	private static final String AGENT_PASS = "ProcessingAgentPass"; //The pass phrase for the receivingAgent
	private MonitoringAgent receivingAgent; //This agent will be responsible for receiving all incoming message
	private Map<Long, String> monitoredServices = new HashMap<Long, String>(); 
	
	private String databaseName = "to be done";
	private int databaseTypeInt = -1; //See SQLDatabaseType for more information
	private	SQLDatabaseType databaseType;
	private String databaseHost = "to be done";
	private int databasePort = -1;
	private String databaseUser = "to be done";
	private String databasePassword = "to be done";
	
	private SQLDatabase database;
	
	public MonitoringDataProcessingService(){
		setFieldValues(); //This sets the values of the property file
		this.databaseType = SQLDatabaseType.getSQLDatabaseType(databaseTypeInt);
		this.database = new SQLDatabase(this.databaseType, this.databaseUser, this.databasePassword,
				this.databaseName, this.databaseHost, this.databasePort);
		try {
			this.database.connect();
			System.out.println("Monitoring: Database connected!");
		} catch (Exception e) {
			System.out.println("Monitoring: Could not connect to database!");
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 
	 * Will be called by the receiving {@link i5.las2peer.security.MonitoringAgent} of this service,
	 * if it receives a message from a monitored node.
	 * 
	 * @param messages an array of {@link i5.las2peer.logging.monitoring.MonitoringMessage}s
	 * 
	 */
	public boolean getMessages(MonitoringMessage[] messages){
		Agent requestingAgent = getActiveAgent();
		if(receivingAgent == null){
			System.out.println("Monitoring: Agent not registered yet, this invokation must be false!");
			return false;
		}
		if(requestingAgent.getId() != receivingAgent.getId()){
			System.out.println("Monitoring: I only take messages from my own Agent!");
			return false;
		}
		System.out.println("Monitoring: Got a monitoring message!");
		return processMessages(messages);
	}
	
	
	private boolean processMessages(MonitoringMessage[] messages) {
		boolean returnStatement = true;
		for(MonitoringMessage message : messages){
			
			// Happens when a node has sent its last messages
			if(message == null){
				return returnStatement;
			}
			
			// Add node to database (running means we got an id representation) 
			else if((message.getEvent() == Event.NODE_STATUS_CHANGE && message.getRemarks().equals("RUNNING"))
					|| message.getEvent() == Event.NEW_NODE_NOTICE){
				returnStatement = persistMessage(message, "NODE");
				if(!returnStatement)
					return returnStatement;
				
				returnStatement = persistMessage(message, "MESSAGE");
				if(!returnStatement)
					return returnStatement;
			}
			
			//Add unregister date to all registered agents at this node
			else if(message.getEvent() == Event.NODE_STATUS_CHANGE && message.getRemarks().equals("CLOSING")){
				returnStatement = persistMessage(message, "REGISTERED_AT");
				if(!returnStatement)
					return returnStatement;
				returnStatement = persistMessage(message, "MESSAGE");
				if(!returnStatement)
					return returnStatement;
			}
			
			// Add service to monitored service list and add service, service agent, 'registered at' and message to database
			else if(message.getEvent() == Event.SERVICE_ADD_TO_MONITORING){
				monitoredServices.put(message.getSourceAgentId(), message.getRemarks());
				returnStatement = persistMessage(message, "AGENT");
				if(!returnStatement)
					return returnStatement;
				
				returnStatement = persistMessage(message, "SERVICE");
				if(!returnStatement)
					return returnStatement;
				
				returnStatement = persistMessage(message, "REGISTERED_AT");
				if(!returnStatement)
					return returnStatement;
				
				returnStatement = persistMessage(message, "MESSAGE");
				if(!returnStatement)
					return returnStatement;
			}
			
			//Add agent to database 
			else if(message.getEvent() == Event.AGENT_REGISTERED && !message.getRemarks().equals("ServiceAgent")){
				returnStatement = persistMessage(message, "AGENT");
				if(!returnStatement)
					return returnStatement;
				
				returnStatement = persistMessage(message, "REGISTERED_AT");
				if(!returnStatement)
					return returnStatement;
				
				returnStatement = persistMessage(message, "MESSAGE");
				if(!returnStatement)
					return returnStatement;
			}
			
			// If enabled for monitoring, add service message to database
			else if(Math.abs(message.getEvent().getCode()) >= 7000 && (Math.abs(message.getEvent().getCode()) < 8000)){
				if(monitoredServices.containsKey(message.getSourceAgentId())){
					returnStatement = persistMessage(message, "MESSAGE");
					if(!returnStatement)
						return returnStatement;
				}
			}
			// Just log the message
			else{
				returnStatement = persistMessage(message, "MESSAGE");
				if(!returnStatement)
					return returnStatement;
			}
			
		}
		return returnStatement;
		
	}
	
	
	private boolean persistMessage(MonitoringMessage message, String table) {
		boolean returnStatement = false;
		try {
			String insertStatement = DatabaseInsertStatement.returnInsertStatement(message, database.getJdbcInfo(), table);
			returnStatement = database.store(insertStatement);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnStatement;
	}
	
	
	/**
	 * 
	 * Returns the id of this monitoring agent (that will be responsible for message receiving).
	 * Creates one if not existent.
	 * 
	 * @param greetings will be printed in the console and is only used to control registering
	 * 
	 * @return the id
	 * 
	 */
	public long getReceivingAgentId(String greetings){
		System.out.println("Monitoring: Service requests receiving agent id: " + greetings);
		if(receivingAgent == null){
			try {
				receivingAgent = MonitoringAgent.createMonitoringAgent(AGENT_PASS);
				receivingAgent.unlockPrivateKey(AGENT_PASS);
				getActiveNode().storeAgent(receivingAgent);
				getActiveNode().registerReceiver(receivingAgent);
			} catch (CryptoException | AgentException | L2pSecurityException e) {
				e.printStackTrace();
			}
		}
		return this.receivingAgent.getId();
	}
}
