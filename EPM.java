import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EPM {

    private static String inputLocation;
    private static String processedLocation;
    private static String errorLocation;
    private static String DBusername;
    private static String DBpassword;
    private static String DBservice;
    private static final String DEFAULT_INPUT_LOCATION = "C:/IntelliJ_Project/EPM/src/Input";
    private static final String DEFAULT_PROCESSED_LOCATION = "C:/IntelliJ_Project/EPM/src/Processed";
    private static final String DEFAULT_ERROR_LOCATION = "C:/IntelliJ_Project/EPM/src/Error";
    private static final int DEFAULT_SLEEP_TIME = 5;
    private static int sleepTime;

    public static void main(String[] args) {

        //Load properties file
        Properties properties = loadPropertiesFile();

        //Get all the values in properties file
        if (!getPropertyValues(properties)) {
            System.out.println("ERROR: Database parameters are empty");
            System.exit(0);
        }

        CSVHandler csvHandler = new CSVHandler(DBusername, DBpassword, DBservice);

        while (true) {

            //Holds file with .txt extension
            ArrayList<File> arrayList = getValidFiles();

            //No files exists in the input directory, so sleep for specified seconds
            if (arrayList.size() == 0) {
                System.out.println("No files located in " + inputLocation);
                System.out.println("Going to sleep for " + sleepTime + " seconds\n");
                try {
                    TimeUnit.SECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                    System.out.println(e);
                }
            } else {
                //Create a thread pool
                ExecutorService executor = Executors.newCachedThreadPool();

                for (File f : arrayList) {
                    String fileName = f.getName();
                    System.out.println("Reading " + fileName);

                    //Cannot parse the file, then move to Error directory
                    if (!csvHandler.parseFile(f, executor)) {
                        System.out.println("Error while parsing " + fileName);
                        if (f.renameTo(new File(errorLocation + "/" + fileName)))
                            System.out.println("Successfully moved to " + errorLocation);
                        continue;
                    }
                    File destFile = new File(processedLocation + "/" + fileName);

                    //If file already exists in destination then delete it, else move from input to destination
                    if (destFile.exists()) {
//                        destFile.delete();
                        String timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
                        destFile = new File(processedLocation + "/" + timeStamp + "_" + fileName);
                    }
                    if (f.renameTo(destFile))
                        System.out.println("Successfully moved to " + processedLocation);

                }
            }

        }
    }

    private static ArrayList<File> getValidFiles() {

        ArrayList<File> arrayList = new ArrayList<>();
        //Get all the files in the input directory
        File file = new File(inputLocation);
        File[] listOfFiles = file.listFiles();

        if (listOfFiles == null) {
            System.out.println("ERROR: " + inputLocation + " is not a directory");
            System.exit(0);
        }

        //Insert all the files ending with .txt in arrayList
        for (File f : listOfFiles) {
            if (f.getName().endsWith(".txt"))
                arrayList.add(f);
            else {
                if (f.renameTo(new File(errorLocation + "/" + f.getName())))
                    System.out.println(f.getName() + " successfully moved to " + errorLocation);
            }
        }
        return arrayList;
    }

    private static boolean getPropertyValues(Properties properties) {

        if ((inputLocation = properties.getProperty(Constants.INPUT_LOCATION)) != null)
            inputLocation = inputLocation.trim();
        else
            inputLocation = DEFAULT_INPUT_LOCATION;

        if ((processedLocation = properties.getProperty(Constants.PROCESSED_LOCATION)) != null)
            processedLocation = processedLocation.trim();
        else
            processedLocation = DEFAULT_PROCESSED_LOCATION;

        if ((errorLocation = properties.getProperty(Constants.ERROR_LOCATION)) != null)
            errorLocation = errorLocation.trim();
        else
            errorLocation = DEFAULT_ERROR_LOCATION;

        String sTime;
        if ((sTime = properties.getProperty(Constants.SLEEP_TIME)) != null) {
            sTime = sTime.trim();
            try {
                sleepTime = Integer.parseInt(sTime);
            } catch (NumberFormatException e) {
                sleepTime = DEFAULT_SLEEP_TIME;
            }
        } else
            sleepTime = DEFAULT_SLEEP_TIME;

        if ((DBusername = properties.getProperty(Constants.DB_USERNAME)) != null &&
                (DBpassword = properties.getProperty(Constants.DB_PASSWORD)) != null &&
                (DBservice = properties.getProperty(Constants.DB_SERVICE)) != null) {
            DBusername = DBusername.trim();
            DBpassword = DBpassword.trim();
            DBservice = DBservice.trim();
            return !DBusername.isEmpty() && !DBpassword.isEmpty() && !DBservice.isEmpty();

        } else
            return false;

    }

    private static Properties loadPropertiesFile() {
        Properties properties = new Properties();
        try (InputStream inputStream = new FileInputStream(Constants.PROPERTIES_FILE_NAME)) {
            properties.load(inputStream);
        } catch (IOException e) {
            System.out.println(e);
            System.exit(0);
        }
        return properties;
    }
}
