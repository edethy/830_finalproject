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

import com.google.auth.oauth2.*;
import com.google.firebase.auth.FirebaseCredential;
import com.google.firebase.auth.FirebaseCredentials;
import com.google.firebase.*;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.*;
// import com.google.api.client.auth.oauth2.*;
import com.google.api.client.json.jackson2.JacksonFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.io.FileInputStream;

public class test {
    public static void sortEdge(String filename) throws InterruptedException, IOException {
        String inputFile = "edge_tmp.txt";
		String outputFile = filename;

		FileReader fileReader = new FileReader(inputFile);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String inputLine;
		List<String> lineList = new ArrayList<String>();
		while ((inputLine = bufferedReader.readLine()) != null) {
			lineList.add(inputLine);
		}
		fileReader.close();

		Collections.sort(lineList, (String r1, String r2) ->
        r1.split(", ")[3].compareTo(r2.split(", ")[3]));

		FileWriter fileWriter = new FileWriter(outputFile);
		PrintWriter out = new PrintWriter(fileWriter);
		for (String outputLine : lineList) {
			out.println(outputLine);
		}
		out.flush();
		out.close();
		fileWriter.close();

    }

    public static void main(String[] argv) throws InterruptedException, IOException {
        if(argv.length < 2) {
            System.out.println("SLAP: Need two args! [input_file_name.txt] [output_file_name.txt]");
            return;
        }
    
        JSONParser parser = new JSONParser();
        try {   
            JSONArray a = (JSONArray) parser.parse(new FileReader("g1.json"));
            PrintWriter nodeTableWriter = new PrintWriter(argv[0], "UTF-8");
            PrintWriter edgeTableWriter = new PrintWriter("edge_tmp.txt", "UTF-8");

            // To check wheter the node is already added to table
            HashMap<Long, Boolean> checkAdded = new HashMap<>();
            Long prevID = 0L,
                prevSince = 0L;
            Boolean first = true;

            int edgeID = 0;

            for (Object o : a)
            {
                JSONObject person = (JSONObject) o;
            
                Long id = (Long) person.get("id");
                String since = (String) person.get("since");
                since = since.split(" GMT")[0];
                Long tabIndex = (Long) person.get("tabIndex");
                Long time = (Long) person.get("time"); // in sec
                // time = time * 60; 
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
                           
                        //    System.out.println("Date, with the default formatting: " + startDateString2);
                           sinceNumber = Long.parseLong(startDateString2);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
                if(!first) {
                    Long threshold = 3000L;   // MMSS
                    // Add an edge only when there are too much time gap bw the current tab and the prev tab
                    if( prevSince + time / 60 * 100 + time % 60  + threshold >= sinceNumber && prevID != id)
                        edgeTableWriter.println(prevID + ", " + id + ", " + edgeID++ + ", " + + sinceNumber);  
                    else if(prevID != id) {  // time break (s, -1), (d, -1)
                        edgeTableWriter.println(prevID + ", -1, " + edgeID++ + ", " + + sinceNumber);
                        edgeTableWriter.println("-1, " + id + ", " + edgeID++ + ", " + sinceNumber);
                    }  
                }
                
                first = false;

                prevID = id;
                prevSince = sinceNumber;

                // If this node is already added, do not add node again
                if( !checkAdded.containsKey(id) ) {
                    checkAdded.put(id, true);
                    title = '"'+ title.split(",")[0] + '"';
                    nodeTableWriter.println(id + ", " + sinceNumber +  ", " + tabIndex + ", " + time + ", " + title + ", " + windowID);              
                }
                
                // System.out.println(name);
                
            }

            nodeTableWriter.close();
            edgeTableWriter.close();


            sortEdge(argv[1]);
            System.out.println("SLAP: graph .txt created at " + argv[0] + ", " + argv[1]);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        
    }

}