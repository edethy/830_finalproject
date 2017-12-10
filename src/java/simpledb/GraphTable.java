package simpledb;
import java.util.HashMap;
import java.io.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.io.PrintWriter;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.*;


public class GraphTable {

    public static void main(String[] argv) {
        // construct table for node & edge
        Type nodeTypes[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.STRING_TYPE, Type.INT_TYPE };
        Type edgeTypes[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE};
        
        String nodeFieldNames[] = new String[]{ "id", "since", "tabIndex", "time", "title", "windowID" };
        String edgeFieldNames[] = new String[]{ "id", "src", "dst", "since"};

        TupleDesc td1 = new TupleDesc(nodeTypes, nodeFieldNames);
        TupleDesc td2 = new TupleDesc(edgeTypes, edgeFieldNames);

        // create the tables, associate them with the data files3
        // and tell the catalog about the schema  the tables.
        HeapFile table1 = new HeapFile(new File("tt.dat"), td1);
        Database.getCatalog().addTable(table1, "node");
        int nodeTableID = Database.getCatalog().getTableId("node");
        System.out.println("NODE table: "+ Database.getCatalog().getTupleDesc(nodeTableID));


        HeapFile table2 = new HeapFile(new File("edge.dat"), td2);
        Database.getCatalog().addTable(table2, "edge");
        int edgeTableID = Database.getCatalog().getTableId("edge");
        System.out.println("EDGE table: "+ Database.getCatalog().getTupleDesc(edgeTableID));
       

    }

}