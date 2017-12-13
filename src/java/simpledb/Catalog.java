package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

	private ConcurrentHashMap<String, DbFile> name_to_file_map;
	private ConcurrentHashMap<String, String> name_to_pkey_map;
	
    private ArrayList<DbFile> file_list;
    
    public String mv_subpaths_table_name = "mv_subpaths";    

    private HashSet<Tuple> materialized_subpaths = new HashSet<Tuple>();    

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
    	file_list = new ArrayList<DbFile>();
    	name_to_file_map = new ConcurrentHashMap<String, DbFile>();
        name_to_pkey_map = new ConcurrentHashMap<String, String>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
		for (String name2 : name_to_file_map.keySet()) {
			if (name_to_file_map.get(name2).equals(file)) {
				name_to_file_map.remove(name2);
			} else if (name_to_file_map.get(name2).getId() == file.getId()) {
				name_to_file_map.remove(name2);
				name_to_pkey_map.remove(name2);
				for (int i=0;i<file_list.size();i++) {
					if (file_list.get(i).getId() == file.getId()) {
						file_list.remove(i);
					}
				}
			}
		}
    	name_to_file_map.put(name, file);
    	name_to_pkey_map.put(name, pkeyField);
    	file_list.add(file);    
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
    	if (name != null && name_to_file_map.containsKey(name)) {
    		DbFile file = name_to_file_map.get(name);
    		return file.getId();
    	}
        throw new NoSuchElementException("table doesn't exist");   
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	for (DbFile file : file_list) {
    		if (file.getId() == tableid) {
    			return file.getTupleDesc();
    		}
    	}
        throw new NoSuchElementException("No File exists with that id");    
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
    	for (DbFile file : file_list) {
    		if (file.getId() == tableid) {
    			return file;
    		}
    	}
        throw new NoSuchElementException("No File exists with that id");
   }

   public boolean tableExists(int tableid) {
        for (DbFile file : file_list) {
    		if (file.getId() == tableid) {
    			return true;
    		}
    	}
        return false;
   }

   public boolean tableExists(String name) {
    	if (name != null && name_to_file_map.containsKey(name)) {
    		return true;
    	}
        return false;
   }

    public String getPrimaryKey(int tableid) {
    	String table_name = this.getTableName(tableid);
    	return name_to_pkey_map.get(table_name);
    }

    public Iterator<Integer> tableIdIterator() {
    	ArrayList<Integer> tableid_list = new ArrayList<Integer>();
    	for (DbFile file : file_list) {
    		tableid_list.add(file.getId());
    	}
    	return tableid_list.iterator();
    }

    public String getTableName(int id) {
        // some code goes here
    	for (DbFile file : file_list) {
    		if (file.getId() == id) {
    			for (String name : name_to_file_map.keySet()) {
    				if (name_to_file_map.get(name).equals(file)) {
    					return name;
    				}
    			}
    		}
    	}
    	return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
    	name_to_file_map.clear();
    	name_to_pkey_map.clear();
    	file_list.clear();
    }
    

    public void loadSubPathMaterializedViews() {
        File mv_subpaths = new File(mv_subpaths_table_name);
        System.out.println("File: " + mv_subpaths.getAbsolutePath());
        MaterializeViewUtil mvutil = new MaterializeViewUtil();
        TupleDesc td = mvutil.getSubPathMVTD();
        HeapFile hf = new HeapFile(mv_subpaths, td);
        addTable(hf, mv_subpaths.getName());

        try {
            int table_id = Database.getCatalog().getTableId(mv_subpaths_table_name);
            SeqScan ss = new SeqScan(new TransactionId(), table_id, "");
            ss.open();
            while(ss.hasNext()) {
                Tuple t = ss.next();
                System.out.println("Adding " + t + " to materialized views");                
                materialized_subpaths.add(t);
            }
            addMVTablesToCatalog();
        } catch(DbException e) {
            e.printStackTrace();
        } catch(TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    private void addMVTablesToCatalog() {
        // try {
            TupleDesc subpath_td = new TupleDesc(new Type[] {Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE},
                                                new String[] {"PathNumber", "PathIndex", "Node", "PageNumber"});
            for (Tuple t : materialized_subpaths) {
                String table_name = t.getField(0).toString();
                File mv_s = new File(table_name);
                HeapFile hf = new HeapFile(mv_s, subpath_td);
                addTable(hf, table_name);
            }

    }

    public void add_mv(Tuple t) {
        System.out.println("Adding tuple " + t + " To materialized views");
        materialized_subpaths.add(t);
    }

    public Tuple get_mv(String expected_query) {
        for (Tuple t : materialized_subpaths) {
            StringField t_query = (StringField)t.getField(1);
            if (t_query.toString().equals(expected_query)) {
                // System.out.println("t query: " )
                return t;
            }
        }
        return null;
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

