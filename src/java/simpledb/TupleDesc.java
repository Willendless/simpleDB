package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable, Iterable<TupleDesc.TDItem> {

    private TDItem[] tupleDescItems;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private class TDIterator implements Iterator<TDItem> {
        int i = 0;

        @Override
        public boolean hasNext() { return i != tupleDescItems.length; }

        @Override
        public TDItem next() { return tupleDescItems[i++]; }
        
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return new TDIterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        assert typeAr.length > 0;
        assert typeAr.length == fieldAr.length;

        tupleDescItems = new TDItem[typeAr.length];

        for (int i = 0; i < typeAr.length; i++) {
            tupleDescItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        assert typeAr.length > 0;

        tupleDescItems = new TDItem[typeAr.length];

        for (int i = 0; i < typeAr.length; i++) {
            tupleDescItems[i] = new TDItem(typeAr[i], "");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() { return tupleDescItems.length; }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= tupleDescItems.length)
            throw new NoSuchElementException("Argument invalid");
        return tupleDescItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= tupleDescItems.length)
            throw new NoSuchElementException("Argument invalid");
        return tupleDescItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < tupleDescItems.length; i++) {
            if (tupleDescItems[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int allSize = 0;
        for (int i = 0; i < this.numFields(); i++)
            allSize += this.getFieldType(i).getLen();
        return allSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int numFields       =   td1.numFields() + td2.numFields();
        int i               =   0;
        Type[] types        =   new Type[numFields];
        String[] strings    =   new String[numFields];

        for (TDItem tdItem : td1) {
            types[i] = tdItem.fieldType;
            strings[i] = tdItem.fieldName;
            i++;
        }

        for (TDItem tdItem : td2) {
            types[i] = tdItem.fieldType;
            strings[i] = tdItem.fieldName;
            i++;
        }

        return new TupleDesc(types, strings);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if (o == null)
            return false;

        if (!o.getClass().equals(this.getClass()))
            return false;

        TupleDesc other = (TupleDesc) o;
        
        if (this.numFields() != other.numFields())
            return false;

        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(other.getFieldType(i)))
                return false;
        }   

        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String res = tupleDescItems[0].fieldType + 
                    "(" + tupleDescItems[0].fieldName + ")";
        for (int i = 1; i < tupleDescItems.length; i++) {
            res = res + ", " + tupleDescItems[i].fieldName + 
                    "(" + tupleDescItems[i].fieldName + ")";
        }
        return res;
    }
}
