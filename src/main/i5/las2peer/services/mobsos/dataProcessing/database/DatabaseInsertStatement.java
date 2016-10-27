package i5.las2peer.services.mobsos.dataProcessing.database;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

import i5.las2peer.logging.NodeObserver.Event;
import i5.las2peer.logging.monitoring.MonitoringMessage;
import i5.las2peer.services.mobsos.dataProcessing.MonitoringMessageWithEncryptedAgents;

/**
 * 
 * Helper Class that provides (static) methods to formulate a SQL statement according to the given specifications. The
 * statements are formulated according to the database scheme that can be found in the "scripts" folder provided with
 * this project.
 *
 * @author Peter de Lange
 *
 */
public class DatabaseInsertStatement {

	/**
	 * 
	 * The entry point to this class.
	 * 
	 * @param db a SQLDatabse connection
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * @param databaseType the database type the statement should be formulated for
	 * @param DB2Schema the schema of the DB2 database (can be set to null if database is MySQL)
	 * @param table the name of the table the query should be inserted to
	 * @param hashRemarks whether you want to hash the remarks or not
	 * 
	 * @return a SQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	public static PreparedStatement returnInsertStatement(SQLDatabase db, MonitoringMessage monitoringMessage,
			SQLDatabaseType databaseType, String DB2Schema, String table, boolean hashRemarks) throws Exception {
		MonitoringMessageWithEncryptedAgents message = new MonitoringMessageWithEncryptedAgents(monitoringMessage,
				hashRemarks);
		if (databaseType == SQLDatabaseType.MySQL) {

			if (table.equals("MESSAGE")) {
				return returnMySQLMessageStatement(db, message);
			}

			else if (table.equals("AGENT")) {
				return returnMySQLAgentStatement(db, message);
			}

			else if (table.equals("SERVICE")) {
				return returnMySQLServiceStatement(db, message);
			}

			else if (table.equals("NODE")) {
				return returnMySQLNodeStatement(db, message);
			}

			else if (table.equals("REGISTERED_AT")) {
				return returnMySQLRegisteredAtStatement(db, message);
			}

			else {
				throw new Exception("Don't know table!");
			}

		}

		else if (databaseType == SQLDatabaseType.DB2) {
			if (table.equals("MESSAGE")) {
				return returnDB2MessageStatement(db, message, DB2Schema);
			}

			else if (table.equals("AGENT")) {
				return returnDB2AgentStatement(db, message, DB2Schema);
			}

			else if (table.equals("SERVICE")) {
				return returnDB2ServiceStatement(db, message, DB2Schema);
			}

			else if (table.equals("NODE")) {
				return returnDB2NodeStatement(db, message, DB2Schema);
			}

			else if (table.equals("REGISTERED_AT")) {
				return returnDB2RegisteredAtStatement(db, message, DB2Schema);
			}

			else {
				throw new Exception("Don't know table!");
			}
		}

		else {
			throw new Exception("Not supported database type!");
		}

	}

	/**
	 * 
	 * Returns a MySQL statement for the message table.
	 * 
	 * @param message a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be
	 *            stored
	 * 
	 * @return a MySQL statement
	 * 
	 */
	private static PreparedStatement returnMySQLMessageStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents message) {
		PreparedStatement statement = null;
		try {
			statement = db.connection
					.prepareStatement("INSERT INTO MESSAGE (`EVENT`, `TIME_STAMP`, `SOURCE_NODE`, `SOURCE_AGENT`, "
							+ "`DESTINATION_NODE`, `DESTINATION_AGENT`, `REMARKS`) VALUES (?,?,?,?,?,?,?);");
			statement.setString(1, message.getEvent().toString()); // EVENT
			statement.setLong(2, message.getTimestamp()); // TIME_STAMP
			if (message.getSourceNode() != null) {
				statement.setString(3, message.getSourceNode().substring(0, 12)); // SOURCE_NODE
			} else {
				statement.setString(3, "");
			}
			if (message.getSourceAgentId() != null) {
				statement.setString(4, message.getSourceAgentId().toString()); // SOURCE_AGENT
			} else {
				statement.setString(4, "");
			}
			if (message.getDestinationNode() != null) {
				statement.setString(5, message.getDestinationNode().substring(0, 12)); // DESTINATION_NODE
			} else {
				statement.setString(5, "");
			}
			if (message.getDestinationAgentId() != null) {
				statement.setString(6, message.getDestinationAgentId().toString()); // DESTINATION_AGENT
			} else {
				statement.setString(6, "");
			}
			if (message.getRemarks() != null) {
				statement.setString(7, message.getRemarks()); // REMARKS
			} else {
				statement.setString(7, "");
			}
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a MySQL statement for the Agent table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * 
	 * @return a MySQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static PreparedStatement returnMySQLAgentStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents monitoringMessage) throws Exception {
		PreparedStatement statement = null;
		try {
			statement = db.connection.prepareStatement(
					"INSERT INTO AGENT(AGENT_ID, TYPE) VALUES(?,?) ON DUPLICATE KEY UPDATE AGENT_ID=AGENT_ID;");

			if (monitoringMessage.getSourceNode() == null || monitoringMessage.getSourceAgentId() == null
					|| monitoringMessage.getRemarks() == null)
				throw new Exception("Missing information for persisting agent entity!");

			String agentType = null;
			if (monitoringMessage.getEvent() == Event.SERVICE_ADD_TO_MONITORING)
				agentType = "SERVICE";
			else if (monitoringMessage.getEvent() == Event.AGENT_REGISTERED) {
				if (monitoringMessage.getRemarks().equals("ServiceAgent")) {
					throw new Exception("ServiceAgents are only persisted when added to monitoring!");
				} else if (monitoringMessage.getRemarks().equals("UserAgent")) {
					agentType = "USER";
				} else if (monitoringMessage.getRemarks().equals("GroupAgent")) {
					agentType = "GROUP";
				} else if (monitoringMessage.getRemarks().equals("MonitoringAgent")) {
					agentType = "MONITORING";
				} else if (monitoringMessage.getRemarks().equals("ServiceInfoAgent")) {
					agentType = "SERVICE_INFO";
				} else if (monitoringMessage.getRemarks().equals("Mediator")) {
					// Thats right, we treat mediators as agents (as from a monitoring point of view, this is the same)
					agentType = "MEDIATOR";
				} else {
					throw new Exception(
							"Unknown remarks entry for persisting agent entity: " + monitoringMessage.getRemarks());
				}
			} else {
				throw new Exception("Agent entities will only be persisted if registered at a node!");
			}
			statement.setString(1, monitoringMessage.getSourceAgentId());
			statement.setString(2, agentType);
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a MySQL statement for the Service table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * 
	 * @return a MySQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static PreparedStatement returnMySQLServiceStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents monitoringMessage) throws Exception {
		PreparedStatement statement = null;
		try {
			statement = db.connection.prepareStatement(
					"INSERT INTO SERVICE(AGENT_ID, SERVICE_CLASS_NAME) VALUES(?,?) ON DUPLICATE KEY UPDATE AGENT_ID=AGENT_ID;");

			if (monitoringMessage.getSourceAgentId() == null || monitoringMessage.getRemarks() == null)
				throw new Exception("Missing information for persisting service entity!");
			statement.setString(1, monitoringMessage.getSourceAgentId());
			statement.setString(2, monitoringMessage.getRemarks());
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a MySQL statement for the "Registered At" table.
	 * 
	 * @param message a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information to be
	 *            stored
	 * 
	 * @return a MySQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static PreparedStatement returnMySQLRegisteredAtStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents message) throws Exception {
		PreparedStatement statement = null;
		try {
			if (message.getTimestamp() == null || message.getSourceNode() == null)
				throw new Exception("Missing information for 'registered at' entity!");

			String timestamp = new Timestamp(message.getTimestamp()).toString();
			String nodeId = message.getSourceNode().substring(0, 12);

			if (message.getEvent() == Event.AGENT_REGISTERED || message.getEvent() == Event.SERVICE_ADD_TO_MONITORING) {
				if (message.getSourceAgentId() == null)
					throw new Exception("Missing information for persisting 'registered at' entity!");
				statement = db.connection.prepareStatement(
						"INSERT INTO REGISTERED_AT(REGISTRATION_DATE, AGENT_ID, RUNNING_AT) VALUES(?, ?, ?);");
				statement.setString(1, timestamp);
				statement.setString(2, message.getSourceAgentId());
				statement.setString(3, nodeId);
			} else if (message.getEvent() == Event.AGENT_REMOVED || message.getEvent() == Event.SERVICE_SHUTDOWN) {
				statement = db.connection.prepareStatement(
						"UPDATE REGISTERED_AT SET UNREGISTRATION_DATE=? WHERE RUNNING_AT=? AND AGENT_ID=? AND UNREGISTRATION_DATE IS NULL;");
				statement.setString(1, timestamp);
				statement.setString(2, nodeId);
				statement.setString(3, message.getSourceAgentId());
			} else { // We need to unregister those who have not yet unregistered -> update statement!
				statement = db.connection.prepareStatement(
						"UPDATE REGISTERED_AT SET UNREGISTRATION_DATE=? WHERE RUNNING_AT=? AND UNREGISTRATION_DATE IS NULL;");
				statement.setString(1, timestamp);
				statement.setString(2, nodeId);
			}
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a MySQL statement for the Node table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * 
	 * @return a MySQL statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static PreparedStatement returnMySQLNodeStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents monitoringMessage) throws Exception {
		PreparedStatement statement = null;
		try {
			statement = db.connection.prepareStatement(
					"INSERT INTO NODE(NODE_ID, NODE_LOCATION) VALUES(?,?) ON DUPLICATE KEY UPDATE NODE_ID = NODE_ID;");
			if (monitoringMessage.getEvent() == Event.NODE_STATUS_CHANGE) {
				if (monitoringMessage.getSourceNode() == null)
					throw new Exception("Missing information for persisting node entity!");
				String nodeId = monitoringMessage.getSourceNode().substring(0, 12);
				int startingLocationPosition = monitoringMessage.getSourceNode().lastIndexOf("/") + 1;
				String nodeLocation = monitoringMessage.getSourceNode().substring(startingLocationPosition);
				statement.setString(1, nodeId);
				statement.setString(2, nodeLocation);
			} else {
				throw new Exception("Node persistence only at new node notice or node creation events!");
			}
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a DB2 statement for the message table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 */
	private static PreparedStatement returnDB2MessageStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents message, String DB2Schema) {
		PreparedStatement statement = null;
		try {
			statement = db.connection.prepareStatement(
					"INSERT INTO " + DB2Schema + ".MESSAGE (EVENT, TIME_STAMP, SOURCE_NODE, SOURCE_AGENT, "
							+ "DESTINATION_NODE, DESTINATION_AGENT, REMARKS) VALUES (?,?,?,?,?,?,?);");

			statement.setString(1, message.getEvent().toString()); // EVENT
			statement.setLong(2, message.getTimestamp()); // TIME_STAMP
			if (message.getSourceNode() != null) {
				statement.setString(3, message.getSourceNode().substring(0, 12)); // SOURCE_NODE
			} else {
				statement.setString(3, "");
			}
			if (message.getSourceAgentId() != null) {
				statement.setString(4, message.getSourceAgentId().toString()); // SOURCE_AGENT
			} else {
				statement.setString(4, "");
			}
			if (message.getDestinationNode() != null) {
				statement.setString(5, message.getDestinationNode().substring(0, 12)); // DESTINATION_NODE
			} else {
				statement.setString(5, "");
			}
			if (message.getDestinationAgentId() != null) {
				statement.setString(6, message.getDestinationAgentId().toString()); // DESTINATION_AGENT
			} else {
				statement.setString(6, "");
			}
			if (message.getRemarks() != null) {
				statement.setString(7, message.getRemarks()); // REMARKS
			} else {
				statement.setString(7, "");
			}
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a DB2 statement for the Agent table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static PreparedStatement returnDB2AgentStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents monitoringMessage, String DB2Schema) throws Exception {
		PreparedStatement statement = null;
		try {
			statement = db.connection.prepareStatement("MERGE INTO " + DB2Schema
					+ ".AGENT agent1 USING (VALUES(?, ?)) AS agent2(AGENT_ID,TYPE) ON agent1.AGENT_ID=agent2.AGENT_ID "
					+ "WHEN MATCHED THEN UPDATE SET agent1.TYPE = agent2.TYPE WHEN NOT MATCHED THEN "
					+ "INSERT (AGENT_ID, TYPE) VALUES(?,?)");

			if (monitoringMessage.getSourceNode() == null || monitoringMessage.getSourceAgentId() == null
					|| monitoringMessage.getRemarks() == null)
				throw new Exception("Missing information for persisting agent entity!");

			String agentType = null;
			if (monitoringMessage.getEvent() == Event.SERVICE_ADD_TO_MONITORING)
				agentType = "SERVICE";
			else if (monitoringMessage.getEvent() == Event.AGENT_REGISTERED) {
				if (monitoringMessage.getRemarks().equals("ServiceAgent")) {
					throw new Exception("ServiceAgents are only persisted when added to monitoring!");
				} else if (monitoringMessage.getRemarks().equals("UserAgent")) {
					agentType = "USER";
				} else if (monitoringMessage.getRemarks().equals("GroupAgent")) {
					agentType = "GROUP";
				} else if (monitoringMessage.getRemarks().equals("MonitoringAgent")) {
					agentType = "MONITORING";
				} else if (monitoringMessage.getRemarks().equals("Mediator")) {
					// Thats right, we treat mediators as agents (as from a monitoring point of view, this is the same)
					agentType = "MEDIATOR";
				} else {
					throw new Exception(
							"Unknown remarks entry for persisting agent entity: " + monitoringMessage.getRemarks());
				}
			} else {
				throw new Exception("Agent entities will only be persisted if registered at a node!");
			}
			statement.setString(1, monitoringMessage.getSourceAgentId());
			statement.setString(2, agentType);

			statement.setString(3, monitoringMessage.getSourceAgentId());
			statement.setString(4, agentType);
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a DB2 statement for the Service table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static PreparedStatement returnDB2ServiceStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents monitoringMessage, String DB2Schema) throws Exception {
		PreparedStatement statement = null;
		try {
			statement = db.connection
					.prepareStatement("MERGE INTO " + DB2Schema + ".SERVICE service1 USING (VALUES(?,?)) "
							+ "AS service2(AGENT_ID,SERVICE_CLASS_NAME) AS service2(AGENT_ID,SERVICE_CLASS_NAME) "
							+ "ON service1.AGENT_ID=service2.AGENT_ID WHEN MATCHED THEN "
							+ "UPDATE SET service1.SERVICE_CLASS_NAME = service2.SERVICE_CLASS_NAME "
							+ "WHEN NOT MATCHED THEN INSERT (AGENT_ID, SERVICE_CLASS_NAME) VALUES(?, ?");

			if (monitoringMessage.getSourceAgentId() == null || monitoringMessage.getRemarks() == null)
				throw new Exception("Missing information for persisting service entity!");
			statement.setString(1, monitoringMessage.getSourceAgentId());
			statement.setString(2, monitoringMessage.getRemarks());

			statement.setString(3, monitoringMessage.getSourceAgentId());
			statement.setString(4, monitoringMessage.getRemarks());
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a DB2 statement for the "Registered At" table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static PreparedStatement returnDB2RegisteredAtStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents message, String DB2Schema) throws Exception {
		PreparedStatement statement = null;
		try {
			if (message.getTimestamp() == null || message.getSourceNode() == null)
				throw new Exception("Missing information for 'registered at' entity!");

			String timestamp = new Timestamp(message.getTimestamp()).toString();
			String nodeId = message.getSourceNode().substring(0, 12);

			if (message.getEvent() == Event.AGENT_REGISTERED || message.getEvent() == Event.SERVICE_ADD_TO_MONITORING) {
				if (message.getSourceAgentId() == null)
					throw new Exception("Missing information for persisting 'registered at' entity!");
				statement = db.connection.prepareStatement("INSERT INTO " + DB2Schema
						+ ".REGISTERED_AT(REGISTRATION_DATE, AGENT_ID, RUNNING_AT) VALUES(?,?,?)");
				statement.setString(1, timestamp);
				statement.setString(2, message.getSourceAgentId());
				statement.setString(3, nodeId);
			} else if (message.getEvent() == Event.AGENT_REMOVED || message.getEvent() == Event.SERVICE_SHUTDOWN) {
				statement = db.connection.prepareStatement("UPDATE " + DB2Schema
						+ ".REGISTERED_AT SET UNREGISTRATION_DATE=? WHERE RUNNING_AT=? AND AGENT_ID=? AND UNREGISTRATION_DATE IS NULL");
				statement.setString(1, timestamp);
				statement.setString(2, nodeId);
				statement.setString(3, message.getSourceAgentId());
			} else { // We need to unregister those who have not yet unregistered -> update statement!
				statement = db.connection.prepareStatement("UPDATE " + DB2Schema
						+ ".REGISTERED_AT SET UNREGISTRATION_DATE=? WHERE RUNNING_AT=? AND UNREGISTRATION_DATE IS NULL");
				statement.setString(1, timestamp);
				statement.setString(2, nodeId);
			}
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

	/**
	 * 
	 * Returns a DB2 statement for the Node table.
	 * 
	 * @param monitoringMessage a {@link i5.las2peer.logging.monitoring.MonitoringMessage} that contains the information
	 *            to be stored
	 * @param DB2Schema the database schema
	 * 
	 * @return a DB2 statement
	 * 
	 * @throws Exception if the given information was not correct or sufficient for the desired table entry
	 * 
	 */
	private static PreparedStatement returnDB2NodeStatement(SQLDatabase db,
			MonitoringMessageWithEncryptedAgents monitoringMessage, String DB2Schema) throws Exception {
		PreparedStatement statement = null;
		try {
			statement = db.connection.prepareStatement(
					"MERGE INTO " + DB2Schema + ".NODE node1 USING (VALUES(?, ?)) AS node2(NODE_ID,NODE_LOCATION) "
							+ "ON node1.NODE_ID=node2.NODE_ID WHEN MATCHED THEN "
							+ "UPDATE SET node1.NODE_LOCATION = node2.NODE_LOCATION "
							+ "WHEN NOT MATCHED THEN INSERT (NODE_ID, NODE_LOCATION) VALUES(?,?)");
			if (monitoringMessage.getEvent() == Event.NODE_STATUS_CHANGE) {
				if (monitoringMessage.getSourceNode() == null)
					throw new Exception("Missing information for persisting node entity!");
				String nodeId = monitoringMessage.getSourceNode().substring(0, 12);
				int startingLocationPosition = monitoringMessage.getSourceNode().lastIndexOf("/") + 1;
				String nodeLocation = monitoringMessage.getSourceNode().substring(startingLocationPosition);
				// Duplicate can happen because of new node notices
				statement.setString(1, nodeId);
				statement.setString(2, nodeLocation);

				statement.setString(3, nodeId);
				statement.setString(4, nodeLocation);
			} else {
				throw new Exception("Node persistence only at new node notice or node creation events!");
			}
		} catch (Exception e) {
			// TODO LOG
			e.printStackTrace();
		}
		return statement;
	}

}