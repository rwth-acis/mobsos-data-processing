package i5.las2peer.services.monitoring.processing.database;

import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;

import java.sql.Timestamp;

public class DatabaseInsertStatement {
	
	public static String returnInsertStatement(MonitoringMessage monitoringMessage, SQLDatabaseType databaseType, String table) throws Exception{		
		if(databaseType == SQLDatabaseType.MySQL){
			
			if(table.equals("MESSAGE")){
				return returnMessageStatement(monitoringMessage, databaseType, table);
			}

			else if(table.equals("AGENT")){
				return returnAgentStatement(monitoringMessage, databaseType, table);
			}
			else if(table.equals("SERVICE")){
				return returnServiceStatement(monitoringMessage, databaseType, table);
			}
			
			else if(table.equals("NODE")){
				return returnNodeStatement(monitoringMessage, databaseType, table);
			}
			
			else if(table.equals("REGISTERED_AT")){
				return returnRegisteredAtStatement(monitoringMessage, databaseType, table);				
			}
			
			else{
				throw new Exception("Don't know table!");
			}
			
		}
		
		else if(databaseType == SQLDatabaseType.DB2){
			throw new Exception("Not implemented yet!");
		}
		
		else{
			throw new Exception("Not supported database type!");
		}
		
	}
	
	
	private static String returnMessageStatement(MonitoringMessage monitoringMessage, SQLDatabaseType databaseType, String table){
		String returnStatement;
					
		String event =  "'" + monitoringMessage.getEvent().toString() + "'";
		String timestamp = ", '" + new Timestamp(monitoringMessage.getTimestamp()).toString() + "'";
		String timespan = "";
		String sourceNode = "";
		String sourceAgentId = "";
		String destinationNode = "";
		String destinationAgentId = "";
		String remarks = "";
		returnStatement = "INSERT INTO MESSAGE (EVENT, TIME_STAMP";
		if(monitoringMessage.getTimespan() != null){
			returnStatement += " ,TIME_SPAN";
			timespan = monitoringMessage.getTimespan().toString();
		}
		if(monitoringMessage.getSourceNode() != null){
			returnStatement += ", SOURCE_NODE";
			sourceNode = ", '" + monitoringMessage.getSourceNode() + "'";
		}
		if(monitoringMessage.getSourceAgentId() != null){
			returnStatement += ", SOURCE_AGENT";
			sourceAgentId = ", " + monitoringMessage.getSourceAgentId().toString();
		}
		if(monitoringMessage.getDestinationNode() != null){
			returnStatement += ", DESTINATION_NODE";
			destinationNode = ", '" + monitoringMessage.getDestinationNode() + "'";
		}
		if(monitoringMessage.getDestinationAgentId() != null){
			returnStatement += ", DESTINATION_AGENT";
			destinationAgentId = ", " + monitoringMessage.getDestinationAgentId().toString();

		}
		if(monitoringMessage.getRemarks() != null){
			returnStatement += ", REMARKS";
			remarks = ", '" + monitoringMessage.getRemarks() + "'";
		}
		returnStatement += ") VALUES(";
		returnStatement += event + timestamp + timespan + sourceNode + sourceAgentId + destinationNode + destinationAgentId + remarks;
		returnStatement += ");";
		return returnStatement;
	}
	
	
	private static String returnAgentStatement(MonitoringMessage monitoringMessage, SQLDatabaseType databaseType, String table) throws Exception{
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
			throw new Exception("Agents will only be persisted if registered at a node!"); 
		}
		returnStatement = "INSERT INTO AGENT(AGENT_ID, TYPE) VALUES(";
		returnStatement += monitoringMessage.getSourceAgentId() + ", '" + agentType + "') ON DUPLICATE KEY UPDATE AGENT_ID=AGENT_ID;";
		return returnStatement;
	}
	
	
	private static String returnServiceStatement(MonitoringMessage monitoringMessage, SQLDatabaseType databaseType, String table) throws Exception {
		String returnStatement;
		
		if(monitoringMessage.getSourceAgentId() == null || monitoringMessage.getRemarks() == null)
			throw new Exception("Missing information for persisting node entity!");
		
		returnStatement = "INSERT INTO SERVICE(AGENT_ID, SERVICE_CLASS_NAME) VALUES(";
		returnStatement += monitoringMessage.getSourceAgentId() + ", '" + monitoringMessage.getRemarks() +"') ON DUPLICATE KEY UPDATE AGENT_ID=AGENT_ID;";
		return returnStatement;
	}
	
	
	private static String returnRegisteredAtStatement(MonitoringMessage monitoringMessage, SQLDatabaseType databaseType, String table) throws Exception {
		String returnStatement;
		String timestamp = "'" + new Timestamp(monitoringMessage.getTimestamp()).toString() + "'";

		if(monitoringMessage.getEvent() == Event.NODE_STATUS_CHANGE && monitoringMessage.getRemarks().equals("CLOSING")){
			//We need to unregister -> update statement!
			returnStatement = "UPDATE REGISTERED_AT SET UNREGISTRATION_DATE=";
			returnStatement += timestamp;
			returnStatement += " WHERE RUNNING_AT='" + monitoringMessage.getSourceNode() + "';";
			return returnStatement;
		}
		
		else{
			if(monitoringMessage.getTimestamp() == null || monitoringMessage.getSourceAgentId() == null || monitoringMessage.getSourceNode() == null)
				throw new Exception("Missing information for persisting 'registered at' entity!");
			returnStatement = "INSERT INTO REGISTERED_AT(REGISTRATION_DATE, AGENT_ID, RUNNING_AT) VALUES(";
			returnStatement += timestamp + ", " + monitoringMessage.getSourceAgentId() + ", '" + monitoringMessage.getSourceNode() +"');";
			return returnStatement;
		}
	}
	
	
	private static String returnNodeStatement(MonitoringMessage monitoringMessage, SQLDatabaseType databaseType, String table) throws Exception {
		String returnStatement;
		
		if(monitoringMessage.getSourceNode() == null)
			throw new Exception("Missing information for persisting node entity!");

		returnStatement = "INSERT INTO NODE(NODE_ID) VALUES(";
		returnStatement += "'" + monitoringMessage.getSourceNode() +"');";
		return returnStatement;
	}

}


