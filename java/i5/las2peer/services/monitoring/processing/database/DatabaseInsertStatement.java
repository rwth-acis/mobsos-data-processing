package i5.las2peer.services.monitoring.processing.database;

import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;

import java.sql.Timestamp;

public class DatabaseInsertStatement {
	
	public static String returnInsertStatement(MonitoringMessage monitoringMessage, SQLDatabaseType databaseType, String table) throws Exception{
		String returnStatement;
		if(databaseType == SQLDatabaseType.MySQL){
			if(table.equals("MESSAGE")){
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
			else if(table.equals("AGENT")){
				if(monitoringMessage.getSourceNode() == null || monitoringMessage.getSourceAgentId() == null)
					throw new Exception("Missing information for persisting agent entity!");
				String agentType = monitoringMessage.getRemarks(); //temp
				if(monitoringMessage.getEvent() == Event.SERVICE_ADD_TO_MONITORING)
					agentType = "SERVICE";
				returnStatement = "INSERT INTO AGENT(AGENT_ID, TYPE, RUNNING_AT) VALUES(";
				returnStatement += monitoringMessage.getSourceAgentId() + ", '" + agentType + "', '" + monitoringMessage.getSourceNode() + "');";
				return returnStatement;
			}
			else if(table.equals("SERVICE")){
				throw new Exception("Not implemented yet");
			}
			else if(table.equals("NODE")){
				if(monitoringMessage.getSourceNode() == null)
					throw new Exception("Missing information for persisting node entity!");
				
				returnStatement = "INSERT INTO NODE(NODE_ID) VALUES(";
				returnStatement += "'" + monitoringMessage.getSourceNode() +"');";
				return returnStatement;

			}
		}
		else if(databaseType == SQLDatabaseType.DB2){
			throw new Exception("Not implemented yet");
		}
		throw new Exception("Not supported database type");
	}
	
	
}
