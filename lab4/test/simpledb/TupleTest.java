package simpledb;

import junit.framework.JUnit4TestAdapter;
import org.junit.Test;
import simpledb.systemtest.SimpleDbTestBase;

import static org.junit.Assert.assertEquals;

public class TupleTest extends SimpleDbTestBase {

    /**
     * Unit test for Tuple.getField() and Tuple.setField()
     */
    @Test
    public void modifyFields() {
        TupleDesc td = Utility.getTupleDesc(2);

        Tuple tup = new Tuple(td);
        tup.setField(0, new IntField(-1));
        tup.setField(1, new IntField(0));

        assertEquals(new IntField(-1), tup.getField(0));
        assertEquals(new IntField(0), tup.getField(1));

        tup.setField(0, new IntField(1));
        tup.setField(1, new IntField(37));

        assertEquals(new IntField(1), tup.getField(0));
        assertEquals(new IntField(37), tup.getField(1));
    }
    @Test
    public void modifyFields2() {
        Type[] tpAr = {Type.INT_TYPE, Type.STRING_TYPE, Type.INT_TYPE, Type.STRING_TYPE};
        String[] fieldAr = {"1", "", "2", "3"};
        TupleDesc td = new TupleDesc(tpAr, fieldAr);

        Tuple tup = new Tuple(td);
        tup.setField(0, new IntField(1000));
        tup.setField(1, new StringField("", 128));
        tup.setField(2, new IntField(-10));
        tup.setField(3, new StringField("shen", 10));
        assertEquals(new IntField(1000), tup.getField(0));
        assertEquals(new StringField("shen", 10), tup.getField(3));

        tup.setField(0, new IntField(1));
        tup.setField(1, new IntField(37));

        assertEquals(new IntField(1), tup.getField(0));
        assertEquals(new IntField(37), tup.getField(1));

    }

    /**
     * Unit test for Tuple.getTupleDesc()
     */
    @Test
    public void getTupleDesc() {
        TupleDesc td = Utility.getTupleDesc(5);
        Tuple tup = new Tuple(td);
        assertEquals(td, tup.getTupleDesc());
    }

    /**
     * Unit test for Tuple.getRecordId() and Tuple.setRecordId()
     */
    @Test
    public void modifyRecordId() {
        Tuple tup1 = new Tuple(Utility.getTupleDesc(1));
        HeapPageId pid1 = new HeapPageId(0,0);
        RecordId rid1 = new RecordId(pid1, 0);
        tup1.setRecordId(rid1);

	try {
	    assertEquals(rid1, tup1.getRecordId());
	} catch (UnsupportedOperationException e) {
		//rethrow the exception with an explanation
    	throw new UnsupportedOperationException("modifyRecordId() test failed due to " +
    			"RecordId.equals() not being implemented.  This is not required for Lab 1, " +
    			"but should pass when you do implement the RecordId class.");
	}
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(TupleTest.class);
    }
}

