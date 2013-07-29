package i5.las2peer.services.monitoring.processing;

import i5.las2peer.api.Service;
import i5.las2peer.communication.Message;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.p2p.AgentNotKnownException;
import i5.las2peer.persistency.EncodingFailedException;
import i5.las2peer.security.L2pSecurityException;
import i5.las2peer.security.UserAgent;
import i5.las2peer.tools.CryptoException;
import i5.las2peer.tools.SerializationException;

import java.util.Random;



/**
 * 
 * MonitoringDataProcessingService.java
 * <br>
 * 
 */
public class MonitoringDataProcessingService extends Service{
	MonitoringMessage[] messages;
	public MonitoringDataProcessingService(){

	}
	
	
	public boolean getMessages(){
		Random r = new Random();
		//TODO, just a test
		try {
			UserAgent testSender = UserAgent.createUserAgent("testPass"); //I know that this is not the final solution;-)
			testSender.unlockPrivateKey("testPass");
			try {
				Message message = new Message(testSender,getAgent(),"test");
			} catch (EncodingFailedException | AgentNotKnownException
					| SerializationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (CryptoException | L2pSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;
	}
}
