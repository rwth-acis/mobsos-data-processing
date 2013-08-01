package i5.las2peer.services.monitoring.processing;

import i5.las2peer.api.Service;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.security.Agent;
import i5.las2peer.security.AgentException;
import i5.las2peer.security.L2pSecurityException;
import i5.las2peer.security.MonitoringAgent;
import i5.las2peer.tools.CryptoException;



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
	
	public MonitoringDataProcessingService(){
		setFieldValues(); //This sets the values of the property file
	}
	
	
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
		System.out.println("Got something, need a database!"); //TODO
		return true;
	}
	
	
	public long getReceivingAgentId(String greetings){
		if(receivingAgent == null){
			System.out.println("Message from invokation: " + greetings);
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
