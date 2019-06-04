package i5.las2peer.services.mobsos.dataProcessing.database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DatabaseQuery {

    public static PreparedStatement returnNodeQueryStatement(Connection con, String nodeID,
                                                             SQLDatabaseType databaseType, String DB2Schema) throws Exception {
        if (databaseType == SQLDatabaseType.MySQL) {
            return returnMySQLNodeStatement(con, nodeID);
        } else if (databaseType == SQLDatabaseType.DB2) {
            return returnDB2NodeStatement(con, nodeID, DB2Schema);
        } else {
            throw new Exception("Not supported database type!");
        }

    }

    private static PreparedStatement returnMySQLNodeStatement(Connection con, String nodeID) {
        PreparedStatement statement = null;
        try {
            statement = con.prepareStatement("SELECT * FROM NODE WHERE NODE_ID = ?");
            statement.setString(1, nodeID);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return statement;
    }

    private static PreparedStatement returnDB2NodeStatement(Connection con, String nodeID, String DB2Schema) {
        PreparedStatement statement = null;
        try {
            statement = con.prepareStatement("SELECT * FROM " + DB2Schema + ".NODE WHERE NODE_ID = ?");
            statement.setString(1, nodeID);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return statement;

    }
}
