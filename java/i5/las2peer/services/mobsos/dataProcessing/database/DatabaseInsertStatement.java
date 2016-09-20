package i5.las2peer.services.mobsos.dataProcessing.database;

import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.services.mobsos.dataProcessing.MonitoringMessageWithEncryptedAgents;

import java.sql.Timestamp;

/**
 * 
 * Helper Class that provides (static) methods to formulate a SQL statement according to the given specifications.
 * The statements are formulated according to the database scheme that can be found in the "scripts" folder provided with this project.
 *
 * @author Peter de Lange
 *
 */
public class DatabaseInsertStatement {
	
	
	/**
	 * 
	 * The entry point to this class.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * @param databaseType the database type the statement should be formulated for
	 * @param DB2Schema the schema of the DB2 database (can be set to null if database is MySQL)
	 * @param table the name of the table the query should be inserted to
	 * 
	 * @return a SQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	public static String returnInsertStatement(MonitoringMessage monitoringMessage, SQLDatabaseType databaseType, String DB2Schema, String table) throws Exception{		
		MonitoringMessageWithEncryptedAgents message = new MonitoringMessageWithEncryptedAgents(monitoringMessage);
		if(databaseType == SQLDatabaseType.MySQL){
			
			if(table.equals("MESSAGE")){
				return returnMySQLMessageStatement(message);
			}
			
			else if(table.equals("AGENT")){
				return returnMySQLAgentStatement(message);
			}
			
			else if(table.equals("SERVICE")){
				return returnMySQLServiceStatement(message);
			}
			
			else if(table.equals("NODE")){
				return returnMySQLNodeStatement(message);
			}
			
			else if(table.equals("REGISTERED_AT")){
				return returnMySQLRegisteredAtStatement(message);				
			}
			
			else{
				throw new Exception("Don't know table!");
			}
			
		}
		
		else if(databaseType == SQLDatabaseType.DB2){
			if(table.equals("MESSAGE")){
				return returnDB2MessageStatement(message, DB2Schema);
			}
			
			else if(table.equals("AGENT")){
				return returnDB2AgentStatement(message, DB2Schema);
			}
			
			else if(table.equals("SERVICE")){
				return returnDB2ServiceStatement(message, DB2Schema);
			}
			
			else if(table.equals("NODE")){
				return returnDB2NodeStatement(message, DB2Schema);
			}
			
			else if(table.equals("REGISTERED_AT")){
				return returnDB2RegisteredAtStatement(message, DB2Schema);				
			}
			
			else{
				throw new Exception("Don't know table!");
			}
		}
		
		else{
			throw new Exception("Not supported database type!");
		}
		
	}
	
	
	/**
	 * 
	 * Returns a MySQL statement for the message table.
	 * 
	 * @param message a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * 
	 * @return a MySQL statement
	 * 
	 */
	private static String returnMySQLMessageStatement(MonitoringMessageWithEncryptedAgents message){
		String returnStatement;
		
		String event =  "'" + message.getEvent().toString() + "'";
		String timestamp = ", '" + message.getTimestamp() + "'";
		String sourceNode = "";
		String sourceAgentId = "";
		String destinationNode = "";
		String destinationAgentId = "";
		String remarks = "";
		returnStatement = "INSERT INTO MESSAGE (EVENT, TIME_STAMP";
		if(message.getSourceNode() != null){
			returnStatement += ", SOURCE_NODE";
			sourceNode = ", '" + message.getSourceNode().substring(0, 12) + "'";
		}
		if(message.getSourceAgentId() != null){
			returnStatement += ", SOURCE_AGENT";
			sourceAgentId = ", '" + message.getSourceAgentId().toString() + "'";
		}
		if(message.getDestinationNode() != null){
			returnStatement += ", DESTINATION_NODE";
			destinationNode = ", '" + message.getDestinationNode().substring(0, 12) + "'";
		}
		if(message.getDestinationAgentId() != null){
			returnStatement += ", DESTINATION_AGENT";
			destinationAgentId = ", '" + message.getDestinationAgentId().toString() + "'";

		}
		if(message.getRemarks() != null){
			returnStatement += ", REMARKS";
			remarks = ", '" + message.getRemarks() + "'";
		}
		returnStatement += ") VALUES(";
		returnStatement += event + timestamp + sourceNode + sourceAgentId + destinationNode + destinationAgentId + remarks;
		returnStatement += ");";
		return returnStatement;
	}
	
	
	/**
	 * 
	 * Returns a MySQL statement for the Agent table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * 
	 * @return a MySQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static String returnMySQLAgentStatement(MonitoringMessageWithEncryptedAgents monitoringMessage) throws Exception{
		String returnStatement;
		
		if(monitoringMessage.getSourceNode() == null || monitoringMessage.getSourceAgentId() == null || monitoringMessage.getRemarks() == null)
			throw new Exception("Missing information for persisting agent entity!");
		
		String agentType = null;
		if(monitoringMessage.getEvent() == Event.SERVICE_ADD_TO_MONITORING)
			agentType = "SERVICE";
		else if(monitoringMessage.getEvent() == Event.AGENT_REGISTERED){
			if(monitoringMessage.getRemarks().equals("ServiceAgent")){
				throw new Exception("ServiceAgents are only persisted when added to monitoring!");
			}
			else if(monitoringMessage.getRemarks().equals("UserAgent")){
				agentType = "USER";
			}
			else if(monitoringMessage.getRemarks().equals("GroupAgent")){
				agentType = "GROUP";
			}
			else if(monitoringMessage.getRemarks().equals("MonitoringAgent")){
				agentType = "MONITORING";
			}
			else if(monitoringMessage.getRemarks().equals("ServiceInfoAgent")){
				agentType = "SERVICE_INFO";
			}
			else if(monitoringMessage.getRemarks().equals("Mediator")){
			//Thats right, we treat mediators as agents (as from a monitoring point of view, this is the same)
				agentType = "MEDIATOR";
			}
			else{
				throw new Exception("Unknown remarks entry for persisting agent entity: " + monitoringMessage.getRemarks());
			}
		}
		else{
			throw new Exception("Agent entities will only be persisted if registered at a node!"); 
		}
		returnStatement = "INSERT INTO AGENT(AGENT_ID, TYPE) VALUES('";
		returnStatement += monitoringMessage.getSourceAgentId() + "', '" + agentType + "') ON DUPLICATE KEY UPDATE AGENT_ID=AGENT_ID;";
		return returnStatement;
	}
	
	
	/**
	 * 
	 * Returns a MySQL statement for the Service table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * 
	 * @return a MySQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static String returnMySQLServiceStatement(MonitoringMessageWithEncryptedAgents monitoringMessage) throws Exception {
		String returnStatement;
		
		if(monitoringMessage.getSourceAgentId() == null || monitoringMessage.getRemarks() == null)
			throw new Exception("Missing information for persisting service entity!");
		
		returnStatement = "INSERT INTO SERVICE(AGENT_ID, SERVICE_CLASS_NAME) VALUES('";
		returnStatement += monitoringMessage.getSourceAgentId() + "', '" + monitoringMessage.getRemarks() +"') ON DUPLICATE KEY UPDATE AGENT_ID=AGENT_ID;";
		return returnStatement;
	}
	
	
	/**
	 * 
	 * Returns a MySQL statement for the "Registered At" table.
	 * 
	 * @param message a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * 
	 * @return a MySQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static String returnMySQLRegisteredAtStatement(MonitoringMessageWithEncryptedAgents message) throws Exception {
		String returnStatement;
		if(message.getTimestamp() == null || message.getSourceNode() == null)
			throw new Exception("Missing information for 'registered at' entity!");
	
		String timestamp = "'" + new Timestamp(message.getTimestamp()).toString() + "'";
		String nodeId = message.getSourceNode().substring(0, 12);
	
		if(message.getEvent() == Event.AGENT_REGISTERED || message.getEvent() == Event.SERVICE_ADD_TO_MONITORING){
			if(message.getSourceAgentId() == null)
				throw new Exception("Missing information for persisting 'registered at' entity!");
			returnStatement = "INSERT INTO REGISTERED_AT(REGISTRATION_DATE, AGENT_ID, RUNNING_AT) VALUES(";
			returnStatement += timestamp + ", '" + message.getSourceAgentId() + "', '" + nodeId +"');";
			return returnStatement;
		}
		else if(message.getEvent() == Event.AGENT_REMOVED || message.getEvent() == Event.SERVICE_SHUTDOWN){
			returnStatement = "UPDATE REGISTERED_AT SET UNREGISTRATION_DATE=";
			returnStatement += timestamp;
			returnStatement += " WHERE RUNNING_AT='" + nodeId + "' AND AGENT_ID='" + message.getSourceAgentId() + "' AND UNREGISTRATION_DATE IS NULL;";
			return returnStatement;
		}
		else{ //We need to unregister those who have not yet unregistered -> update statement!
			returnStatement = "UPDATE REGISTERED_AT SET UNREGISTRATION_DATE=";
			returnStatement += timestamp;
			returnStatement += " WHERE RUNNING_AT='" + nodeId + "' AND UNREGISTRATION_DATE IS NULL;";
			return returnStatement;
		}
		
	}
	
	
	/**
	 * 
	 * Returns a MySQL statement for the Node table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * 
	 * @return a MySQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static String returnMySQLNodeStatement(MonitoringMessageWithEncryptedAgents monitoringMessage) throws Exception {
		String returnStatement;
		if(monitoringMessage.getEvent() == Event.NODE_STATUS_CHANGE){
			if(monitoringMessage.getSourceNode() == null)
				throw new Exception("Missing information for persisting node entity!");
			String nodeId = monitoringMessage.getSourceNode().substring(0, 12);
			int startingLocationPosition = monitoringMessage.getSourceNode().lastIndexOf("/") + 1;
			String nodeLocation = monitoringMessage.getSourceNode().substring(startingLocationPosition);
			returnStatement = "INSERT INTO NODE(NODE_ID, NODE_LOCATION) VALUES(";
			returnStatement += "'" + nodeId +"', '" + nodeLocation + "') ON DUPLICATE KEY UPDATE NODE_ID = NODE_ID;";
			return returnStatement;
		}
		else{
			throw new Exception("Node persistence only at new node notice or node creation events!");
		}
	}
	
	
	/**
	 * 
	 * Returns a DB2 statement for the message table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 */
	private static String returnDB2MessageStatement(MonitoringMessageWithEncryptedAgents monitoringMessage, String DB2Schema){
		String returnStatement;
		
		String event =  "'" + monitoringMessage.getEvent().toString() + "'";
		String timestamp = ", " + monitoringMessage.getTimestamp();
		String sourceNode = "";
		String sourceAgentId = "";
		String destinationNode = "";
		String destinationAgentId = "";
		String remarks = "";
		returnStatement = "INSERT INTO " + DB2Schema + ".MESSAGE (EVENT, TIME_STAMP";
		if(monitoringMessage.getSourceNode() != null){
			returnStatement += ", SOURCE_NODE";
			sourceNode = ", '" + monitoringMessage.getSourceNode().substring(0, 12) + "'";
		}
		if(monitoringMessage.getSourceAgentId() != null){
			returnStatement += ", SOURCE_AGENT";
			sourceAgentId = ", '" + monitoringMessage.getSourceAgentId().toString() + "'";
		}
		if(monitoringMessage.getDestinationNode() != null){
			returnStatement += ", DESTINATION_NODE";
			destinationNode = ", '" + monitoringMessage.getDestinationNode().substring(0, 12) + "'";
		}
		if(monitoringMessage.getDestinationAgentId() != null){
			returnStatement += ", DESTINATION_AGENT";
			destinationAgentId = ", '" + monitoringMessage.getDestinationAgentId().toString() + "'";
		}
		if(monitoringMessage.getRemarks() != null){
			returnStatement += ", REMARKS";
			remarks = ", '" + monitoringMessage.getRemarks() + "'";
		}
		returnStatement += ") VALUES(";
		returnStatement += event + timestamp + sourceNode + sourceAgentId + destinationNode + destinationAgentId + remarks;
		returnStatement += ")";
		return returnStatement;
	}
	
	
	/**
	 * 
	 * Returns a DB2 statement for the Agent table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static String returnDB2AgentStatement(MonitoringMessageWithEncryptedAgents monitoringMessage, String DB2Schema) throws Exception{
		String returnStatement;
		
		if(monitoringMessage.getSourceNode() == null || monitoringMessage.getSourceAgentId() == null || monitoringMessage.getRemarks() == null)
			throw new Exception("Missing information for persisting agent entity!");
		
		String agentType = null;
		if(monitoringMessage.getEvent() == Event.SERVICE_ADD_TO_MONITORING)
			agentType = "SERVICE";
		else if(monitoringMessage.getEvent() == Event.AGENT_REGISTERED){
			if(monitoringMessage.getRemarks().equals("ServiceAgent")){
				throw new Exception("ServiceAgents are only persisted when added to monitoring!");
			}
			else if(monitoringMessage.getRemarks().equals("UserAgent")){
				agentType = "USER";
			}
			else if(monitoringMessage.getRemarks().equals("GroupAgent")){
				agentType = "GROUP";
			}
			else if(monitoringMessage.getRemarks().equals("MonitoringAgent")){
				agentType = "MONITORING";
			}
			else if(monitoringMessage.getRemarks().equals("Mediator")){
			//Thats right, we treat mediators as agents (as from a monitoring point of view, this is the same)
				agentType = "MEDIATOR";
			}
			else{
				throw new Exception("Unknown remarks entry for persisting agent entity: " + monitoringMessage.getRemarks());
			}
		}
		else{
			throw new Exception("Agent entities will only be persisted if registered at a node!"); 
		}
		
		
		returnStatement = "MERGE INTO " + DB2Schema + ".AGENT agent1 USING ";
		returnStatement += "(VALUES('" + monitoringMessage.getSourceAgentId() + "', '" + agentType + "')) ";
		returnStatement += "AS agent2(AGENT_ID,TYPE) ";
		returnStatement += "ON agent1.AGENT_ID=agent2.AGENT_ID WHEN MATCHED THEN UPDATE SET agent1.TYPE = agent2.TYPE ";
		returnStatement += "WHEN NOT MATCHED THEN INSERT (AGENT_ID, TYPE) ";
		returnStatement += "VALUES('" + monitoringMessage.getSourceAgentId() + "', '" + agentType + "')";
		return returnStatement;
	}
	
	
	/**
	 * 
	 * Returns a DB2 statement for the Service table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static String returnDB2ServiceStatement(MonitoringMessageWithEncryptedAgents monitoringMessage, String DB2Schema) throws Exception {
		String returnStatement;
		
		if(monitoringMessage.getSourceAgentId() == null || monitoringMessage.getRemarks() == null)
			throw new Exception("Missing information for persisting service entity!");
		
		returnStatement = "MERGE INTO " + DB2Schema + ".SERVICE service1 USING ";
		returnStatement += "(VALUES('" + monitoringMessage.getSourceAgentId() + "', '" + monitoringMessage.getRemarks() + "')) ";
		returnStatement += "AS service2(AGENT_ID,SERVICE_CLASS_NAME) ";
		returnStatement += "ON service1.AGENT_ID=service2.AGENT_ID WHEN MATCHED THEN UPDATE SET service1.SERVICE_CLASS_NAME = service2.SERVICE_CLASS_NAME ";
		returnStatement += "WHEN NOT MATCHED THEN INSERT (AGENT_ID, SERVICE_CLASS_NAME) ";
		returnStatement += "VALUES('" + monitoringMessage.getSourceAgentId() + "', '" + monitoringMessage.getRemarks() + "')";
		return returnStatement;
	}
	
	
	/**
	 * 
	 * Returns a DB2 statement for the "Registered At" table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static String returnDB2RegisteredAtStatement(MonitoringMessageWithEncryptedAgents monitoringMessage, String DB2Schema) throws Exception {
		String returnStatement;
		if(monitoringMessage.getTimestamp() == null || monitoringMessage.getSourceNode() == null)
			throw new Exception("Missing information for 'registered at' entity!");
	
		String timestamp = "'" + new Timestamp(monitoringMessage.getTimestamp()).toString() + "'";
		String nodeId = monitoringMessage.getSourceNode().substring(0, 12);
	
		if(monitoringMessage.getEvent() == Event.AGENT_REGISTERED || monitoringMessage.getEvent() == Event.SERVICE_ADD_TO_MONITORING){
			if(monitoringMessage.getSourceAgentId() == null)
				throw new Exception("Missing information for persisting 'registered at' entity!");
			returnStatement = "INSERT INTO " + DB2Schema + ".REGISTERED_AT(REGISTRATION_DATE, AGENT_ID, RUNNING_AT) VALUES(";
			returnStatement += timestamp + ", '" + monitoringMessage.getSourceAgentId() + "', '" + nodeId +"')";
			return returnStatement;
		}
		else if(monitoringMessage.getEvent() == Event.AGENT_REMOVED || monitoringMessage.getEvent() == Event.SERVICE_SHUTDOWN){
			returnStatement = "UPDATE " + DB2Schema + ".REGISTERED_AT SET UNREGISTRATION_DATE=";
			returnStatement += timestamp;
			returnStatement += " WHERE RUNNING_AT='" + nodeId + "' AND AGENT_ID='" + monitoringMessage.getSourceAgentId() + "' AND UNREGISTRATION_DATE IS NULL";
			return returnStatement;
		}
		else{ //We need to unregister those who have not yet unregistered -> update statement!
			returnStatement = "UPDATE " + DB2Schema + ".REGISTERED_AT SET UNREGISTRATION_DATE=";
			returnStatement += timestamp;
			returnStatement += " WHERE RUNNING_AT='" + nodeId + "' AND UNREGISTRATION_DATE IS NULL";
			return returnStatement;
		}
		
	}
	
	
	/**
	 * 
	 * Returns a DB2 statement for the Node table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static String returnDB2NodeStatement(MonitoringMessageWithEncryptedAgents monitoringMessage, String DB2Schema) throws Exception {
		String returnStatement;
		if(monitoringMessage.getEvent() == Event.NODE_STATUS_CHANGE){
			if(monitoringMessage.getSourceNode() == null)
				throw new Exception("Missing information for persisting node entity!");
			String nodeId = monitoringMessage.getSourceNode().substring(0, 12);
			int startingLocationPosition = monitoringMessage.getSourceNode().lastIndexOf("/") + 1;
			String nodeLocation = monitoringMessage.getSourceNode().substring(startingLocationPosition);
			//Duplicate can happen because of new node notices
			returnStatement = "MERGE INTO " + DB2Schema + ".NODE node1 USING ";
			returnStatement += "(VALUES('" + nodeId + "', '" + nodeLocation + "')) ";
			returnStatement += "AS node2(NODE_ID,NODE_LOCATION) ";
			returnStatement += "ON node1.NODE_ID=node2.NODE_ID WHEN MATCHED THEN UPDATE SET node1.NODE_LOCATION = node2.NODE_LOCATION ";
			returnStatement += "WHEN NOT MATCHED THEN INSERT (NODE_ID, NODE_LOCATION) ";
			returnStatement += "VALUES('" + nodeId + "', '" + nodeLocation + "')";
			return returnStatement;
		}
		else{
			throw new Exception("Node persistence only at new node notice or node creation events!");
		}
	}
	
}
