package i5.las2peer.services.mobsos.dataProcessing;

import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * 
 * Data class that takes a {@link i5.las2peer.logging.monitoring.MonitoringMessage} and
 * encrypts the source and destination agents as an MD5 hash-string for privacy reasons.
 * Service Messages will have an encrypted remarks field as well to prevent service 
 * developers from gathering user specific information via their custom service
 * messages.
 * This is how data will be persisted in the database later on.
 * 
 * @author Peter de Lange
 *
 */
public class MonitoringMessageWithEncryptedAgents {
	
	
	private Long timestamp;
	private Event event;
	private String sourceNode;
	private String sourceAgentId = null;
	private String destinationNode;
	private String destinationAgentId = null;
	private String remarks;
	
	
	/**
	 * 
	 * Constructor of a MonitoringMessageWithEncryptedAgents.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage}
	 * 
	 */
	public MonitoringMessageWithEncryptedAgents(MonitoringMessage monitoringMessage){
		this.timestamp = monitoringMessage.getTimestamp();
		this.event = monitoringMessage.getEvent();
		this.sourceNode = monitoringMessage.getSourceNode();
		if(monitoringMessage.getSourceAgentId() != null)
			this.sourceAgentId = DigestUtils.md5Hex((monitoringMessage.getSourceAgentId().toString()));
		this.destinationNode = monitoringMessage.getDestinationNode();
		if(monitoringMessage.getDestinationAgentId() != null)
			this.destinationAgentId = DigestUtils.md5Hex((monitoringMessage.getDestinationAgentId().toString()));
		//Custom service messages
		if(Math.abs(this.getEvent().getCode()) >= 7500 && (Math.abs(this.getEvent().getCode()) < 7600)){
			this.remarks = DigestUtils.md5Hex((monitoringMessage.getRemarks()));
		}
		else{
			this.remarks = monitoringMessage.getRemarks();
		}
	}
	
	
	public Long getTimestamp() {
		return timestamp;
	}
	
	
	public Event getEvent() {
		return event;
	}
	
	
	public String getSourceNode() {
		return sourceNode;
	}
	
	
	public String getSourceAgentId() {
		return sourceAgentId;
	}
	
	public String getDestinationNode() {
		return destinationNode;
	}
	
	
	public String getDestinationAgentId() {
		return destinationAgentId;
	}
	
	
	public String getRemarks() {
		return remarks;
	}
	
	
}
