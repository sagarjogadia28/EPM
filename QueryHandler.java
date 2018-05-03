import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

class QueryHandler implements Runnable {
    private Connection connection;
    private static final String EVENT_RATE_TABLE = "EVENT_RATE";
    private static final String SUBS_OFFER_TABLE = "SUBSCRIBER_OFFER";
    private static final String ACCEPT_TABLE = "EPM_RATED_EVENT";
    private static final String REJECT_TABLE = "EPM_REJECTED_EVENT";
    private PreparedStatement preparedStatement;
    private ArrayList<RatedEvent> arrayList;
    private DBConnection dbConnection;

    QueryHandler(String DBusername, String DBpassword, String DBservice) {
        dbConnection = new DBConnection();
        if (!dbConnection.establishConnection(DBusername, DBpassword, DBservice)) {
            System.out.println("Cannot establish connection to database");
            System.exit(0);
        }
        connection = dbConnection.getConnection();
    }

    @Override
    public void run() {
        //Process query for each line in arrayList
        for (RatedEvent ratedEvent : arrayList) {
            processQuery(ratedEvent);
        }
        dbConnection.closeConnection();
    }

    private void processQuery(RatedEvent ratedEvent) {
        try {
            String FETCH_QUERY = "SELECT OFFER_ID, EFFECTIVE_DATE FROM ( SELECT * FROM " + SUBS_OFFER_TABLE + " WHERE SUBSCR_RESOURCE LIKE ? and EFFECTIVE_DATE <= TO_DATE( ?, 'dd/mm/yyyy hh24:mi:ss') ORDER BY EFFECTIVE_DATE DESC) WHERE ROWNUM = 1";
            preparedStatement = connection.prepareStatement(FETCH_QUERY);
            preparedStatement.setString(1, ratedEvent.getEventType());
            preparedStatement.setString(2, ratedEvent.getEventStartTime());
            ResultSet resultSet = preparedStatement.executeQuery();

            //If offer_id exists, then check in event_rate table else insert in rejected table
            if (resultSet.next()) {
                String QUERY = "SELECT * FROM " + EVENT_RATE_TABLE + " WHERE EVENT_TYPE LIKE ? and EFFECTIVE_DATE = TO_DATE(?, 'dd/mm/yyyy hh24:mi:ss') and OFFER_ID = ?";
                preparedStatement = connection.prepareStatement(QUERY);
                DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                preparedStatement.setString(1, ratedEvent.getEventType());
                preparedStatement.setString(2, dateFormat.format(resultSet.getDate("EFFECTIVE_DATE")));
                preparedStatement.setDouble(3, resultSet.getDouble("OFFER_ID"));
                resultSet = preparedStatement.executeQuery();

                //If entry exists in event_rate then insert in accepted table, else in rejected table
                if (resultSet.next()) {
                    insertAccepted(ratedEvent, resultSet);
                } else {
                    String reason = "Cannot find the rate for the event since EVENT_START_TIME < EFFECTIVE_DATE of all entries in EVENT_RATE table";
                    insertRejected(ratedEvent, reason);
                }
            } else {
                String reason = "Cannot find the OFFER_ID for the event";
                insertRejected(ratedEvent, reason);
            }

            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void insertRejected(RatedEvent ratedEvent, String reason) {
        try {
            String QUERY = "INSERT INTO " + REJECT_TABLE + " VALUES ( ?, ?, TO_DATE( ?, 'DD/MM/YYYY HH:MI:SS'), ?, ?)";
            preparedStatement = connection.prepareStatement(QUERY);
            preparedStatement.setString(1, ratedEvent.getEventType());
            preparedStatement.setString(2, ratedEvent.getTargetResource());
            preparedStatement.setString(3, ratedEvent.getEventStartTime());
            preparedStatement.setDouble(4, ratedEvent.getEventUnitConsumed());
            preparedStatement.setString(5, reason);
            ResultSet resultSet = preparedStatement.executeQuery();
            connection.commit();
            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            System.out.println(e);
        }

    }

    private void insertAccepted(RatedEvent ratedEvent, ResultSet resultSet) {
        try {
            double unitAmount = resultSet.getDouble("UNIT_AMOUNT");
            double unitRate = resultSet.getDouble("UNIT_RATE");
            double eventUnitConsumed = ratedEvent.getEventUnitConsumed();

            if (unitAmount <= 0 || unitRate <= 0 || eventUnitConsumed < 0) {
                String reason = "Invalid amount exists in the table";
                insertRejected(ratedEvent, reason);
            }

            double totalCharge = (eventUnitConsumed / unitAmount) * unitRate;

            String QUERY = "INSERT INTO " + ACCEPT_TABLE + " VALUES ( ?, ?, TO_DATE( ?, 'DD/MM/YYYY HH:MI:SS'), ?, ?)";
            preparedStatement = connection.prepareStatement(QUERY);
            preparedStatement.setString(1, ratedEvent.getEventType());
            preparedStatement.setString(2, ratedEvent.getTargetResource());
            preparedStatement.setString(3, ratedEvent.getEventStartTime());
            preparedStatement.setDouble(4, eventUnitConsumed);
            preparedStatement.setDouble(5, totalCharge);
            resultSet = preparedStatement.executeQuery();
            connection.commit();
            resultSet.close();
            preparedStatement.close();
        } catch (SQLException e) {
            System.out.println("ERROR: Cannot insert in the table " + ACCEPT_TABLE);
            System.out.println(e);
        }
    }

    void setArrayList(ArrayList<RatedEvent> arrayList) {
        this.arrayList = arrayList;
    }
}
