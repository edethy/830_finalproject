package simpledb;

import static org.junit.Assert.assertEquals;

import java.util.NoSuchElementException;
import java.util.Random;

import junit.framework.Assert;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.TestUtil.SkeletonFile;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class MaterializeViewTest extends SimpleDbTestBase {
	private static Random r = new Random();
    private static String name = SystemTestUtil.getUUID();
    private static int id1 = r.nextInt();
    private static int id2 = r.nextInt();
	private String nameThisTestRun;

    /**
     * Unit test for Catalog.getTupleDesc()
     */
    @Test public void insertTable() throws Exception {
        OpIterator op = new TestUtil.MockScan(-5, 2, 1);
        MaterializeView mv = new MaterializeView("temp/TestTable","SELECT * FROM data",op);
    }

    @Test public void testSubPathsView() throws Exception {
        // We need to give an OpIterator, let's make it realistic-ish
        OpIterator op = new TestUtil.MockScan(-5, 2, 1);
        MaterializeView mv = new MaterializeView("temp/TestSubPath_0", "haha", op);
/***
 * String node_name, String edge_name, Field start_node_val,
                                    Field target_node_val, int val_field, Predicate.Op target_op,
                                    Predicate.Op start_op
 */

        mv.materializeSubPaths("node_0","edge_0",new IntField(5),new IntField(7), 0, Predicate.Op.EQUALS,Predicate.Op.EQUALS);

        int table_id = Database.getCatalog().getTableId("temp/TestSubPath_0");
        System.out.println("Test TableId: " + table_id);
        int other_table_id = Database.getCatalog().getTableId("mv_subpaths");
        System.out.println("Test SubPath TableId: " + other_table_id);
        SeqScan subpaths_ss = new SeqScan(new TransactionId(), other_table_id, "");

        subpaths_ss.open();
        while(subpaths_ss.hasNext()) {
            Tuple ss_tup = subpaths_ss.next();
            assertEquals(ss_tup.getField(0).toString(), "temp/TestSubPath_0");
        }

        SeqScan tuple_scan = new SeqScan(new TransactionId(), table_id, "");
        tuple_scan.open();
        while(tuple_scan.hasNext()) {
            Tuple t = tuple_scan.next();
            System.out.println("Next Tuple:" + t);
        }
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(MaterializeViewTest.class);
    }
}

