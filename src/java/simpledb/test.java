package simpledb;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.PrintWriter;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.*;
public class test {

    public static void main(String[] argv) {
        try {
            
        } catch (Exception e) {
            e.printStackTrace();
        }

                    

        JSONParser parser = new JSONParser();
        try {   
            

            JSONArray a = (JSONArray) parser.parse(new FileReader("g1.json"));
            PrintWriter writer = new PrintWriter("tt.txt", "UTF-8");

            for (Object o : a)
            {
                JSONObject person = (JSONObject) o;
            
                Long id = (Long) person.get("id");
                String since = (String) person.get("since");
                since = since.split(" GMT")[0];
                Long tabIndex = (Long) person.get("tabIndex");
                Long time = (Long) person.get("time");
                String title = (String) person.get("title");
                Long windowID = (Long) person.get("windowID");

                Long sinceNumber = 0L;

                // Parse since field
                try {
                    // This object can interpret strings representing dates in the format MM/dd/yyyy
                    DateFormat df = new SimpleDateFormat("EEE MMM dd yyyy hh:mm:ss"); 
                
                            DateFormat df2 = new SimpleDateFormat("yyyyMMddhhmmss"); 
                            
                           // Convert from String to Date
                           Date startDate = df.parse(since);
                           String startDateString2 = df2.format(startDate);
                           
                           System.out.println("Date, with the default formatting: " + startDateString2);
                           sinceNumber = Long.parseLong(startDateString2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
                writer.println(id + ", " + sinceNumber +  ", " + tabIndex + ", " + time + ", " + title + ", " + windowID);
                
                // System.out.println(name);
                
            }

            writer.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}