package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;
import java.util.*;

import org.junit.Before;
import org.junit.Test;

import simpledb.SubPaths;
import simpledb.systemtest.SimpleDbTestBase;

public class SubPathsTest extends SimpleDbTestBase {

  int testWidth = 2;
  OpIterator edge_table;
  OpIterator node_table;


  @Test public void testBasicPath() throws Exception {
      // Test Graph of for A->B->C->D->E->F->G->A->F->C
      // Should return [A, B, C] and [A, F, C]
	  String[] edge_tups = new String[] {"A","B","B","C","C","D","D","E","E","F","F","G","G","A","A","F","F","C"};
	  Object[] node_tups = new Object[] {"A", 2, "B", 3, "C", 4, "D",10, "E",6,"F",30,"G",0};

	  edge_table = TestUtil.createTupleList(testWidth, edge_tups);
	  node_table = TestUtil.createTupleList(testWidth, node_tups);



	  StringField start_node = new StringField("A",2);
	  IntField end_node_field_val = new IntField(10);

	  int node_pk_field = 0;
	  int target_node_field = 1;
	  int target_node_join_field = 1;
	  Predicate.Op target_node_op = Predicate.Op.EQUALS;
	  this.edge_table.rewind();
      this.node_table.rewind();
      
      SubPaths subpath_search = new SubPaths(node_table, edge_table, new StringField("A",1), new StringField("C",1), 0, Predicate.Op.EQUALS, Predicate.Op.EQUALS);

      subpath_search.open();
      while(subpath_search.hasNext()) {
          Tuple next_tup = subpath_search.next();
          System.out.println("Next Tuple: " + next_tup);
      }
  }

  @Test public void testLinkedPropPath() throws Exception {
    // Test Graph of for A->B->C->D->E->F->G->A->F->C
    // Should return [A, B, C] and [A, F, C]
    // Tab indexed 0 1 2 3 4 6 9 5
    // facebook.com nytimes.com db.csail.com gmail.com google.com reddit.com
    Object[] edge_tups = new Object[] {0,1,1,2,2,3,3,4,4,0,0,2,2,6,6,8,8,0,0,5};
    Object[] node_tups = new Object[] {0,"facebook.com",1,"nytimes.com",2,"db.csail.com",3,"facebook.com",
                                       4,"reddit.com", 5,"google.com",6,"gmail.com",7,"buzzfeed.com",
                                       8,"smittenkitchen.com"};

    edge_table = TestUtil.createTupleList(testWidth, edge_tups);
    node_table = TestUtil.createTupleList(testWidth, node_tups);



    StringField start_node = new StringField("db.csail.com",64);
    StringField end_node_field_val = new StringField("facebook.com", 64);

    int node_pk_field = 0;
    int target_node_field = 1;
    int target_node_join_field = 1;
    Predicate.Op target_node_op = Predicate.Op.EQUALS;
    this.edge_table.rewind();
    this.node_table.rewind();
    
    SubPaths subpath_search = new SubPaths(node_table, edge_table, new StringField("db.csail.com",64), new StringField("facebook.com",64), 1, Predicate.Op.EQUALS, Predicate.Op.EQUALS);

    subpath_search.open();
    while(subpath_search.hasNext()) {
        Tuple next_tup = subpath_search.next();
        System.out.println("Next Tuple: " + next_tup);
    }
}

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(SubPathsTest.class);
  }
}