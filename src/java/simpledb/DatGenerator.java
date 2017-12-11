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


public class DatGenerator {
    public HeapFile node;
    public HeapFile edge;

    public static void main(String[] args) {
        new GraphTable();
        System.out.println(">> .dat successfully converted");
    }

}