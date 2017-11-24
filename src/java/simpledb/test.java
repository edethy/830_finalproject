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
public class test {

    public static void main(String[] argv) {
        JSONParser parser = new JSONParser();
        try {   

            JSONArray a = (JSONArray) parser.parse(new FileReader("g1.json"));
            PrintWriter writer = new PrintWriter("tt.txt", "UTF-8");

            for (Object o : a)
            {
                JSONObject person = (JSONObject) o;
            
                Long id = (Long) person.get("id");
                String since = (String) person.get("since");
                Long tabIndex = (Long) person.get("tabIndex");
                Long time = (Long) person.get("time");
                String title = (String) person.get("title");
                Long windowID = (Long) person.get("windowID");
                
                writer.println(id + ", " + since +  ", " + tabIndex + ", " + time + ", " + title + ", " + windowID);
                
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