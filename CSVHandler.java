import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;

class CSVHandler {
    private String DBusername, DBpassword, DBservice;
    private static final int NO_OF_LINES = 50;

    CSVHandler(String DBusername, String DBpassword, String DBservice) {
        this.DBusername = DBusername;
        this.DBpassword = DBpassword;
        this.DBservice = DBservice;
    }

    private boolean isFileReady(File file) {

        try {
            Scanner scanner = new Scanner(file);
            scanner.close();
            return true;
        } catch (FileNotFoundException e) {
            return false;
        }
    }

    boolean parseFile(File file, ExecutorService executor) {
        BufferedReader bufferedReader = null;

        while (!isFileReady(file)) ;
        try {
            bufferedReader = new BufferedReader(new FileReader(file));
            String line;
            double unitConsumed;
            ArrayList<RatedEvent> arrayList = new ArrayList<>();

            //Read each line in csv file
            while ((line = bufferedReader.readLine()) != null) {

                //If empty line then ignore
                if (line.isEmpty())
                    continue;

                RatedEvent ratedEvent = new RatedEvent();

                //Remove leading or trailing spaces and split into key and value
                line = line.trim();
                String[] values = line.split(Constants.CSV_DELIMITER);
                boolean isEmpty = trimAllValues(values);

                if (isEmpty || values.length != 5) {
                    System.out.println("Invalid number of values in CSV");
                    continue;
                }

                try {
                    unitConsumed = Double.parseDouble(values[3]);

                } catch (NumberFormatException e) {
                    System.out.println(e);
                    System.out.println("Unit consumed is not a proper number");
                    System.out.println("Skipping...");
                    continue;
                }

                //Set all values in RatedEvent and add to arrayList
                ratedEvent.setEventType(values[0]);
                ratedEvent.setTargetResource(values[1]);
                ratedEvent.setEventStartTime(values[2]);
                ratedEvent.setEventUnitConsumed(unitConsumed);
                ratedEvent.setReceiver(values[4]);
                arrayList.add(ratedEvent);

                //If size of array reaches 50 then create a thread to handle insert queries
                if (arrayList.size() == NO_OF_LINES) {
                    createThread(executor, arrayList);
                }
            }
            //If array still contains RatedEvent
            if (arrayList.size() != 0) {
                createThread(executor, arrayList);
            }

            return true;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {

            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void createThread(ExecutorService executor, ArrayList<RatedEvent> arrayList) {
        QueryHandler queryHandler = new QueryHandler(DBusername, DBpassword, DBservice);
        queryHandler.setArrayList(new ArrayList<>(arrayList));
        executor.execute(queryHandler);
        arrayList.clear();
    }

    private boolean trimAllValues(String[] values) {
        for (int i = 0; i < values.length; i++) {
            values[i] = values[i].trim();
            if (values[i].isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
