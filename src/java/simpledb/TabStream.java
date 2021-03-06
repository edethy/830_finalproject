package simpledb;
import java.io.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import com.google.auth.oauth2.*;
import com.google.firebase.auth.FirebaseCredential;
import com.google.firebase.auth.FirebaseCredentials;
import com.google.firebase.*;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.*;
import com.google.api.client.json.jackson2.JacksonFactory;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.*;

import java.io.FileInputStream;

public class TabStream {
    private static DatabaseReference database;
    private static GraphTable graph;

    public static int parseDate(String since) {
        int sinceNumber = 0;

        // Parse since field
        try {
            // This object can interpret strings representing dates in the format MM/dd/yyyy
            DateFormat df = new SimpleDateFormat("EEE MMM dd yyyy hh:mm:ss"); 
            DateFormat df2 = new SimpleDateFormat("MMddhhmmss"); 
                    
            // Convert from String to Date
            Date startDate = df.parse(since);
            String startDateString2 = df2.format(startDate);
            sinceNumber = Integer.parseInt(startDateString2);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return sinceNumber;
    }

    public static boolean containsNode(int id) throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(new TransactionId(), graph.node.getId(), "");
        Predicate p = new Predicate(0, Predicate.Op.EQUALS, new IntField(id));
    	Filter nodes = new Filter(p, ss);

    	nodes.open();
        
    	if(nodes.hasNext()) {
    		Tuple next_node = nodes.next();
            System.out.println("Next Edge " + next_node);
            
            nodes.close();
            return true;
        }

        nodes.close();
        return false;
    }

    public static void main(String[] args) throws InterruptedException, IOException, DbException {
        // Create a table from .json
        graph = new GraphTable();

        // Set up firebase to open up RT stream
        try {
            FileInputStream serviceAccount = new FileInputStream("slap-3eac9-firebase-adminsdk-1k81v-e5e0d9dcb9.json");
            FirebaseOptions options = new FirebaseOptions.Builder()
                .setDatabaseUrl("https://slap-3eac9.firebaseio.com")
                .setCredential(FirebaseCredentials.fromCertificate(serviceAccount))
                .build();

            FirebaseApp.initializeApp(options);

        } catch(FileNotFoundException e) {
            e.printStackTrace();
        }
        
        // Shared Database reference
        database = FirebaseDatabase.getInstance().getReference();

        // Attach a listener to read the data at our posts reference
        database.addChildEventListener(new ChildEventListener() {
            @Override
            public void onChildAdded(DataSnapshot dataSnapshot, String prevChildKey)  {
                if(dataSnapshot.child("id").getValue() == null) return;

                String id = dataSnapshot.child("id").getValue().toString();
                int idNumber = Integer.parseInt(id) + 10000; // to avoid duplicate 

                try {
                    if(containsNode(idNumber)) return; 
                } catch (DbException e) {
                    e.printStackTrace();
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                }
                
                String since = dataSnapshot.child("since").getValue().toString();
                since = since.split(" GMT")[0];
                int sinceNumber = parseDate(since);

                String tabIndex = dataSnapshot.child("tabIndex").getValue().toString();

                String time = dataSnapshot.child("time").getValue().toString();
                int timeNumber = Integer.parseInt(time);
                timeNumber *= 60;

                String windowID = dataSnapshot.child("windowID").getValue().toString();

                // /************* UPDATE table here. */
                // //insertTuple(TransactionId tid, int tableId, Tuple t) @buffer pool
                // //catalog.tableID(), (HeapFile)Database.getCatalog(), Catalog.getTableId(String name)
                // //tid : new TranactionID
                    
                // //int,int,int,int,string,int
                TupleDesc nodeTD = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.STRING_TYPE, Type.INT_TYPE}, new String[] {"id", "since", "tabIndex", "time", "title", "windowID"});
                Tuple newNode = new Tuple(nodeTD);
                newNode.setField(0, new IntField(idNumber)); // id
                newNode.setField(1, new IntField(sinceNumber)); // since
                newNode.setField(2, new IntField(Integer.parseInt(tabIndex))); // tabIndex
                newNode.setField(3, new IntField(timeNumber)); // time
                newNode.setField(4, new StringField((String)dataSnapshot.child("title").getValue(), 100)); // title(url)
                newNode.setField(5, new IntField(Integer.parseInt(windowID))); // windowID

                // System.out.println("id field: "+ newNode.getField(0));
                // System.out.println("since field: "+ newNode.getField(1));
                // System.out.println("tabIndex field: "+ newNode.getField(2));
                // System.out.println("time field: "+ newNode.getField(3));
                // System.out.println("url field: "+ newNode.getField(4));
                // System.out.println("windowid field: "+ newNode.getField(5));

                try {
                    Database.getBufferPool().insertTuple(new TransactionId(), Database.getCatalog().getTableId("node"), newNode);

                    // Test a new tuple inserted 
                    // if(containsNode(idNumber)) System.out.println("New tuple successfully inserted!!");
                    // else System.out.println("Warning!! New tuple not inserted!!");

                } catch(DbException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TransactionAbortedException e) {
                    e.printStackTrace();
                }
                /************* */
            }
        
            @Override
            public void onChildChanged(DataSnapshot dataSnapshot, String prevChildKey) {}
        
            @Override
            public void onChildRemoved(DataSnapshot dataSnapshot) {}
        
            @Override
            public void onChildMoved(DataSnapshot dataSnapshot, String prevChildKey) {}
        
            @Override
            public void onCancelled(DatabaseError databaseError) {}
        });

        // Receive a query request from visualization. 
        GraphParser gp = new GraphParser();
        // gp.processQuery(cmd);

        while(true) {}
    }
}