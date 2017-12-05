package simpledb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.JUnit4TestAdapter;
import java.util.*;

import org.junit.Before;
import org.junit.Test;

import simpledb.systemtest.SimpleDbTestBase;

public class DFSJoinTest extends SimpleDbTestBase {

  int testWidth = 2;
  OpIterator edge_table;
  OpIterator node_table;


  @Test public void testBasicPath() throws Exception {
	  // Test Graph of for A->B->C->D return [A, B, C, D] for start_node=A, end_node=D
      // Nodes: A 1 B 2 C 3 D 4; 
      // Edges A B B C C D
      // Want to go from "A" to node with target field value = 4
	  String[] edge_tups = new String[] {"A","B","B","C","C","D"};
	  Object[] node_tups = new Object[] {"A", 2, "B", 3, "C", 4, "D",10};

	  edge_table = TestUtil.createTupleList(testWidth, edge_tups);
	  node_table = TestUtil.createTupleList(testWidth, node_tups);

	  StringField start_node = new StringField("A",1);
	  IntField end_node_field_val = new IntField(10);

	  int node_pk_field = 0;
	  int target_node_field = 1;
	  int target_node_join_field = 1;
	  Predicate.Op target_node_op = Predicate.Op.EQUALS;
	  this.edge_table.rewind();
	  this.node_table.rewind();

	  DFSJoin dfs_join_search = new DFSJoin(node_table, edge_table, start_node, node_pk_field, target_node_op,
	  										end_node_field_val, target_node_field, target_node_join_field);
	  
	  HashSet<ArrayList<Field>> paths = dfs_join_search.getPaths();
	  System.out.println("Paths: " + paths);

	  HashSet<ArrayList<Field>> expected_set = new HashSet<ArrayList<Field>>();
	  ArrayList<Field> expected_path = new ArrayList<Field>();
	  expected_path.add(new StringField("A", 1));
	  expected_path.add(new StringField("B", 1));
	  expected_path.add(new StringField("C", 1));	  
	  expected_path.add(new StringField("D", 1));
	  expected_set.add(expected_path);
	  assertEquals(expected_set, paths);
  }

    @Test public void testStopReachValidNode() throws Exception {
	  // Test Graph of for A->B->C->D return [A, B, C, D] for start_node=A, end_node=D
      // Nodes: A 1 B 2 C 3 D 4; 
      // Edges A B B C C D
      // Want to go from "A" to node with target field value = 4
	  String[] edge_tups = new String[] {"A","B","B","C","C","D"};
	  Object[] node_tups = new Object[] {"A", 2, "B", 3, "C", 20, "D",10};

	  edge_table = TestUtil.createTupleList(testWidth, edge_tups);
	  node_table = TestUtil.createTupleList(testWidth, node_tups);

	  StringField start_node = new StringField("A",1);
	  IntField end_node_field_val = new IntField(20);

	  int node_pk_field = 0;
	  int target_node_field = 1;
	  int target_node_join_field = 1;
	  Predicate.Op target_node_op = Predicate.Op.EQUALS;
	  this.edge_table.rewind();
	  this.node_table.rewind();

	  DFSJoin dfs_join_search = new DFSJoin(node_table, edge_table, start_node, node_pk_field, target_node_op,
	  										end_node_field_val, target_node_field, target_node_join_field);

	  HashSet<ArrayList<Field>> paths = dfs_join_search.getPaths();
	  System.out.println("Paths: " + paths);

	  HashSet<ArrayList<Field>> expected_set = new HashSet<ArrayList<Field>>();
	  ArrayList<Field> expected_path = new ArrayList<Field>();
	  expected_path.add(new StringField("A", 1));
	  expected_path.add(new StringField("B", 1));
	  expected_path.add(new StringField("C", 1));	  
	  expected_set.add(expected_path);
	  assertEquals(expected_set, paths);
  }
  
 

  /**
   * JUnit suite target
   */
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(DFSJoinTest.class);
  }
}