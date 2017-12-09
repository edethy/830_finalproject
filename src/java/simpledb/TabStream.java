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

/**
 * Hello world!
 */
public class TabStream {
    private static DatabaseReference database;

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
        database.addValueEventListener(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                // Post post = dataSnapshot.getValue(Post.class);
                System.out.println("added");
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                System.out.println("The read failed: " + databaseError.getCode());
            }
        });

        while(true) {}
    }
}