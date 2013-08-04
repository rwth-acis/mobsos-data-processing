package i5.las2peer.services.monitoring.processing;

import i5.las2peer.api.Service;
import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.security.Agent;
import i5.las2peer.security.AgentException;
import i5.las2peer.security.L2pSecurityException;
import i5.las2peer.security.MonitoringAgent;
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
	private	String databaseType = "to be done";
	private String databaseHost = "to be done";
	private int databasePort = 123;
	private String databaseUser = "to be done";
	private String databasePassword = "to be done";
	
	
	public MonitoringDataProcessingService(){
		setFieldValues(); //This sets the values of the property file
		//TODO: Setup database here
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
			if(message == null) //Happens when the node has sent its last messages
				return returnStatement;
			if(message.getEvent() == Event.SERVICE_ADD_TO_MONITORING){
				monitoredServices.put(message.getSourceAgentId(), message.getRemarks());
			}
			else if(Math.abs(message.getEvent().getCode()) >= 7000 && (Math.abs(message.getEvent().getCode()) < 8000)){ //Service Messages
				if(monitoredServices.containsKey(message.getSourceAgentId())){
					returnStatement = persistMessage(message);
					if(!returnStatement)
						return returnStatement;
				}
			}
			else{
				returnStatement = persistMessage(message);
				if(!returnStatement)
					return returnStatement;
			}
		}
		return returnStatement;
	}
	
	
	private boolean persistMessage(MonitoringMessage message) {
		// TODO store Message
		System.out.println("Monitoring: Persisting message with type " + message.getEvent());
		return true;
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
