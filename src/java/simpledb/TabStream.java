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

import java.io.FileInputStream;

public class TabStream {
    private static DatabaseReference database;

    class Node {
        Long id;
        String since;
        Long tabIndex;
        Long time;
        String title;
        Long windowID;

        public Node() {

        }

        public Node(Long id, String since, Long tabIndex, Long time, String title, Long windowID) {
            System.out.println(id);
            this.id = id;
            this.since = since.split(" GMT")[0];
            this.tabIndex = tabIndex;
            this.time = time * 60;
            this.title = title;
            this.windowID = windowID;
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException {
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
            public void onChildAdded(DataSnapshot dataSnapshot, String prevChildKey) {
                //one row
                //dataSnapshot.child("title").getValue();

                System.out.println(dataSnapshot.child("title").getValue());
                // for (DataSnapshot d: dataSnapshot.getChildren()) {
                //     String name = (String) d.getValue();
                //     System.out.println(name);
                // }
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

        while(true) {}
    }
}