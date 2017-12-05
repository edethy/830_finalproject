package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;
import java.util.*;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class BFSTest extends SimpleDbTestBase {

  int testWidth = 2;
  OpIterator edge_table;

  @Test public void testBasicPath() throws Exception {
	  // Test Graph of for A->B->C->D return [A, B, C, D] for start_node=A, end_node=D
	  String[] tup_data = new String[] {"A","B","B","C","C","D"};
	  edge_table = TestUtil.createTupleList(testWidth, tup_data);
	  StringField start_node = new StringField("A",1);
	  StringField end_node = new StringField("D",1);
	  this.edge_table.rewind();
	  BFS bfs = new BFS(start_node, end_node, this.edge_table, 0, 1);
	  HashSet<ArrayList<Field>> paths = bfs.getPaths();
	  System.out.println("Returned HashSet " + paths);
	  HashSet<ArrayList<Field>> expected_set = new HashSet<ArrayList<Field>>();
	  ArrayList<Field> expected_path = new ArrayList<Field>();
	  expected_path.add(new StringField("A", 1));
	  expected_path.add(new StringField("B", 1));
	  expected_path.add(new StringField("C", 1));	  
	  expected_path.add(new StringField("D", 1));
	  expected_set.add(expected_path);
	  assertEquals(expected_set, paths);
  }
  
  
  @Test public void testTerminateAtEndNode() throws Exception {
	  // Test Graph A->B->C->D->E->F
	  //						 ->
	  String[] tup_data = new String[] {"A","B","B","C","C","D","D","E","E","F"};
	  edge_table = TestUtil.createTupleList(testWidth, tup_data);
	  StringField start_node = new StringField("A",1);
	  StringField end_node = new StringField("D",1);
	  BFS bfs = new BFS(start_node, end_node, this.edge_table, 0, 1);
	  HashSet<ArrayList<Field>> paths = bfs.getPaths();
	  System.out.println("Returned HashSet " + paths);
	  HashSet<ArrayList<Field>> expected_set = new HashSet<ArrayList<Field>>();
	  ArrayList<Field> expected_path = new ArrayList<Field>();
	  expected_path.add(new StringField("A", 1));
	  expected_path.add(new StringField("B", 1));
	  expected_path.add(new StringField("C", 1));	  
	  expected_path.add(new StringField("D", 1));
	  expected_set.add(expected_path);
	  assertEquals(expected_set, paths);
 }
 
 @Test public void testMultiplePathsToEndNode() throws Exception  {
	 //Test Graph with 2 possibilities from C->D
	 // C->D or C->F->D
	 String[] tup_data = new String[] {"A","B","B","C","C","D","G","H","G","A","G","Z","F","D","C","F"};
	 edge_table = TestUtil.createTupleList(testWidth, tup_data);
	 StringField start_node = new StringField("A",1);
	 StringField end_node = new StringField("D",1);
	 BFS bfs = new BFS(start_node, end_node, this.edge_table, 0, 1);
	 HashSet<ArrayList<Field>> paths = bfs.getPaths();
	 System.out.println("Returned HashSet " + paths);
	 HashSet<ArrayList<Field>> expected_set = new HashSet<ArrayList<Field>>();
	 
	 ArrayList<Field> expected_path_abcd = new ArrayList<Field>();
	 expected_path_abcd.add(new StringField("A", 1));
	 expected_path_abcd.add(new StringField("B", 1));
	 expected_path_abcd.add(new StringField("C", 1));	  
	 expected_path_abcd.add(new StringField("D", 1));
	 expected_set.add(expected_path_abcd);
	 
	 ArrayList<Field> expected_path_abcfd = new ArrayList<Field>();
	 expected_path_abcfd.add(new StringField("A", 1));
	 expected_path_abcfd.add(new StringField("B", 1));
	 expected_path_abcfd.add(new StringField("C", 1));	  
	 expected_path_abcfd.add(new StringField("F", 1));
	 expected_path_abcfd.add(new StringField("D", 1));
	 
	 expected_set.add(expected_path_abcfd);
	 assertEquals(expected_set, paths);
 }
 
 @Test public void testBackTrackPath() throws Exception  {
	 //Test Graph with 2 possibilities from C->D
	 // C->D or C->F->D
	 String[] tup_data = new String[] {"A","B","B","C","C","D","B","E","E","G","G","B"};
	 edge_table = TestUtil.createTupleList(testWidth, tup_data);
	 StringField start_node = new StringField("A",1);
	 StringField end_node = new StringField("D",1);
	 BFS bfs = new BFS(start_node, end_node, this.edge_table, 0, 1);
	 HashSet<ArrayList<Field>> paths = bfs.getPaths();
	 System.out.println("Returned HashSet " + paths);
	 HashSet<ArrayList<Field>> expected_set = new HashSet<ArrayList<Field>>();
	 
	 ArrayList<Field> expected_path_abcd = new ArrayList<Field>();
	 expected_path_abcd.add(new StringField("A", 1));
	 expected_path_abcd.add(new StringField("B", 1));
	 expected_path_abcd.add(new StringField("C", 1));	  
	 expected_path_abcd.add(new StringField("D", 1));
	 expected_set.add(expected_path_abcd);
	 
	 ArrayList<Field> expected_path_abcfd = new ArrayList<Field>();
	 expected_path_abcfd.add(new StringField("A", 1));
	 expected_path_abcfd.add(new StringField("B", 1));
	 expected_path_abcfd.add(new StringField("C", 1));	  
	 expected_path_abcfd.add(new StringField("F", 1));
	 expected_path_abcfd.add(new StringField("D", 1));
	 
	 expected_set.add(expected_path_abcfd);
	 assertEquals(expected_set, paths);
 }
 
 

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(BFSTest.class);
  }
}