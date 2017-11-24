package simpledb;
import java.util.HashMap;
import java.io.*;
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
        JSONParser parser = new JSONParser();
        try {   
            

            JSONArray a = (JSONArray) parser.parse(new FileReader("g1.json"));
            PrintWriter nodeTableWriter = new PrintWriter("tt.txt", "UTF-8");
            PrintWriter edgeTableWriter = new PrintWriter("edge.txt", "UTF-8");

            // To check wheter the node is already added to table
            HashMap<Long, Boolean> checkAdded = new HashMap<>();
            Long prevID = 0L,
                prevSince = 0L;
            Boolean first = true;

            for (Object o : a)
            {
                JSONObject person = (JSONObject) o;
            
                Long id = (Long) person.get("id");
                String since = (String) person.get("since");
                since = since.split(" GMT")[0];
                Long tabIndex = (Long) person.get("tabIndex");
                Long time = (Long) person.get("time");
                time = time * 60; // in sec
                String title = (String) person.get("title");
                Long windowID = (Long) person.get("windowID");

                Long sinceNumber = 0L;

                // Parse since field
                try {
                    // This object can interpret strings representing dates in the format MM/dd/yyyy
                    DateFormat df = new SimpleDateFormat("EEE MMM dd yyyy hh:mm:ss"); 
                    DateFormat df2 = new SimpleDateFormat("MMddhhmmss"); 
                            
                           // Convert from String to Date
                           Date startDate = df.parse(since);
                           String startDateString2 = df2.format(startDate);
                           
                           System.out.println("Date, with the default formatting: " + startDateString2);
                           sinceNumber = Long.parseLong(startDateString2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
                if(!first) {
                    Long threshold = 500L;   // in minute
                    // Add an edge only there are too much time gap bw the current tab and the prev tab
                    if( prevSince + time + threshold >= sinceNumber && prevID != id)
                        edgeTableWriter.println(prevID + ", " + id);  
                }
                
                first = false;

                prevID = id;
                prevSince = sinceNumber;

                // If this node is already added, do not add node again
                if( !checkAdded.containsKey(id) ) {
                    checkAdded.put(id, true);
                    
                    nodeTableWriter.println(id + ", " + sinceNumber +  ", " + tabIndex + ", " + time + ", " + title + ", " + windowID);              
                }
                
                // System.out.println(name);
                
            }

            nodeTableWriter.close();
            edgeTableWriter.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }


        // construct table for node & edge
        Type nodeTypes[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.STRING_TYPE, Type.INT_TYPE };
        Type edgeTypes[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE};
        
        String nodeFieldNames[] = new String[]{ "id", "since", "tabIndex", "time", "title", "windowID" };
        String edgeFieldNames[] = new String[]{ "src", "dst"};

        TupleDesc td1 = new TupleDesc(nodeTypes, nodeFieldNames);
        TupleDesc td2 = new TupleDesc(edgeTypes, edgeFieldNames);

        // create the tables, associate them with the data files3
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("tt.dat"), td1);
        Database.getCatalog().addTable(table1, "t1");

        HeapFile table2 = new HeapFile(new File("edge.dat"), td2);
        Database.getCatalog().addTable(table2, "t2");
    }

}