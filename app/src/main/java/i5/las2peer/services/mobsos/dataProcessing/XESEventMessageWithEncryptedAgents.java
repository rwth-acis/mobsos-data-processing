package i5.las2peer.services.mobsos.dataProcessing;

import org.apache.commons.codec.digest.DigestUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import i5.las2peer.api.logging.MonitoringEvent;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.logging.monitoring.XESEventMessage;

/**
 * 
 * Data class that takes a
 * {@link i5.las2peer.logging.monitoring.MonitoringMessage} and encrypts the
 * source and
 * destination agents as an MD5 hash-string for privacy reasons. Service
 * Messages will have an encrypted remarks field
 * as well to prevent service developers from gathering user specific
 * information via their custom service messages.
 * This is how data will be persisted in the database later on.
 * 
 * @author Peter de Lange
 *
 */
public class XESEventMessageWithEncryptedAgents extends MonitoringMessageWithEncryptedAgents {

	private String caseId;
	private String activityName;
	private String resourceId;
	private String resourceType;
	private String lifecyclePhase;

	/**
	 * 
	 * Constructor of a MonitoringMessageWithEncryptedAgents.
	 * 
	 * @param eventMessage a
	 *                     {@link i5.las2peer.logging.monitoring.MonitoringMessage}
	 * @param hashRemarks  Whether you want to hash the remarks or not
	 * 
	 */
	public XESEventMessageWithEncryptedAgents(XESEventMessage eventMessage, boolean hashRemarks) {
		super(eventMessage, hashRemarks);
		this.caseId = eventMessage.getCaseId();
		this.activityName = eventMessage.getActivityName();
		this.resourceId = eventMessage.getResourceId();
		this.resourceType = eventMessage.getResourceType();
		this.lifecyclePhase = eventMessage.getLifecyclePhase();
	}

	public String getCaseId() {
		return caseId;
	}

	public String getActivityName() {
		return activityName;
	}

	public String getResourceId() {
		return resourceId;
	}

	public String getResourceType() {
		return resourceType;
	}

	public String getLifecyclePhase() {
		return lifecyclePhase;
	}
}
