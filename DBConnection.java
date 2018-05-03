import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

class DBConnection {

    private Connection connection;

    boolean establishConnection(String DBusername, String DBpassword, String DBservice) {

        String HOST = "10.238.161.146";
        String PORT = "1521";

        try {
            // Load the driver class
            Class.forName(Constants.DRIVER_CLASS);

            // Create the connection object
            connection = DriverManager.getConnection("jdbc:oracle:thin:@" + HOST + ":" + PORT + ":" + DBservice, DBusername, DBpassword);
            return true;

        } catch (ClassNotFoundException | SQLException e) {
            System.out.println(e);
            return false;
        }
    }

    Connection getConnection() {
        return connection;
    }

    void closeConnection(){
        try {
            connection.close();
        } catch (SQLException e) {
            System.out.println(e);
        }
    }
}
