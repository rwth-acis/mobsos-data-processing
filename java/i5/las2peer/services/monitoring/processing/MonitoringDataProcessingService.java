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
 * 
 */
public class MonitoringDataProcessingService extends Service{
	private static final String AGENT_PASS = "ProcessingAgentPass";
	private MonitoringAgent receivingAgent;
	public MonitoringDataProcessingService(){
		
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
				receivingAgent = MonitoringAgent.createReceivingMonitoringAgent(AGENT_PASS);
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
