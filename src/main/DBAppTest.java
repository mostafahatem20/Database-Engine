package Notsosoftware;

import java.awt.Polygon;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Vector;

public class DBAppTest {
	public static void testSelect(Iterator i) {
		while (i.hasNext()) {
			Hashtable<String, Object> c = (Hashtable<String, Object>) i.next();
			System.out.print(c.get("id") + ", ");
			System.out.print(c.get("gpa") + ", ");
			System.out.print(c.get("name") + ", ");
			int[] x = ((Polygon) c.get("shape")).xpoints;
			int[] y = ((Polygon) c.get("shape")).ypoints;
			for (int p = 0; p < x.length; p++) {
				System.out.print("x" + p + "= " + x[p] + ", ");
				System.out.print("y" + p + "= " + y[p] + ", ");
			}

			System.out.println(c.get("TouchDate"));
		}
	}

	public static void test(String tableName) throws ClassNotFoundException {
		DBApp app = new DBApp();
		int l = app.lastFile(tableName);
		String fileName;
		Vector<?> vector = new Vector<Object>();
		for (int i = 1; i <= l; i++) {
			fileName = "data/" + tableName + i + ".class";
			try {
				FileInputStream fileIn = new FileInputStream(fileName);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				vector = (Vector<?>) in.readObject();
				in.close();
				fileIn.close();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			for (int j = 0; j < vector.size(); j++) {
				System.out.print(i + " ");
				System.out.print(((Hashtable<?, ?>) vector.get(j)).get("id") + ", ");
				System.out.print(((Hashtable<?, ?>) vector.get(j)).get("gpa") + ", ");
				System.out.print(((Hashtable<?, ?>) vector.get(j)).get("name") + ", ");
				int[] x = ((Polygon) ((Hashtable<?, ?>) vector.get(j)).get("shape")).xpoints;
				int[] y = ((Polygon) ((Hashtable<?, ?>) vector.get(j)).get("shape")).ypoints;
				for (int p = 0; p < x.length; p++) {
					System.out.print("x" + p + "= " + x[p] + ", ");
					System.out.print("y" + p + "= " + y[p] + ", ");
				}

				System.out.println(((Hashtable<?, ?>) vector.get(j)).get("TouchDate"));

			}

			try {
				FileOutputStream fileOut = new FileOutputStream("data/" + tableName + i + ".class");
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(vector);
				out.close();
				fileOut.close();
				// System.out.println("Serialized data is saved");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public static void tes(String tableName) throws ClassNotFoundException {
		DBApp app = new DBApp();
		int l = app.lastFile(tableName);
		String fileName;
		Vector<?> vector = new Vector<Object>();
		for (int i = 1; i <= l; i++) {
			fileName = "data/" + tableName + i + ".class";
			try {
				FileInputStream fileIn = new FileInputStream(fileName);
				ObjectInputStream in = new ObjectInputStream(fileIn);
				vector = (Vector<?>) in.readObject();
				in.close();
				fileIn.close();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			for (int j = 0; j < vector.size(); j++) {
				System.out.print(i + " ");
				System.out.print(((Hashtable<?, ?>) vector.get(j)).get("id") + ", ");
				System.out.print(((Hashtable<?, ?>) vector.get(j)).get("pass") + ", ");
				System.out.print(((Hashtable<?, ?>) vector.get(j)).get("date") + ", ");
				int[] x = ((Polygon) ((Hashtable<?, ?>) vector.get(j)).get("shape")).xpoints;
				int[] y = ((Polygon) ((Hashtable<?, ?>) vector.get(j)).get("shape")).ypoints;
				for (int p = 0; p < x.length; p++) {
					System.out.print("x" + p + "= " + x[p] + ", ");
					System.out.print("y" + p + "= " + y[p] + ", ");
				}

				System.out.println(((Hashtable<?, ?>) vector.get(j)).get("TouchDate"));

			}

			try {
				FileOutputStream fileOut = new FileOutputStream("data/" + tableName + i + ".class");
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				out.writeObject(vector);
				out.close();
				fileOut.close();
				// System.out.println("Serialized data is saved");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static BPTree test2(String f) throws ClassNotFoundException {
		DBApp app = new DBApp();
		String fileName;
		BPTree b = null;
		fileName = "data/" + f + ".class";
		try {
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			b = (BPTree) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException e) {
			e.printStackTrace();

		}

		try {
			FileOutputStream fileOut = new FileOutputStream("data/" + f + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(b);
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return b;
	}

	public static RTree test3(String f) throws ClassNotFoundException {
		DBApp app = new DBApp();
		String fileName;
		RTree b = null;
		fileName = "data/" + f + ".class";
		try {
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			b = (RTree) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException e) {
			e.printStackTrace();

		}

		try {
			FileOutputStream fileOut = new FileOutputStream("data/" + f + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(b);
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved");
		} catch (IOException e) {
			e.printStackTrace();
		}
		return b;
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, ParseException, DBAppException {
		String strTableName = "Studenten";
		BPTree<Integer> tree = new BPTree<Integer>(2);
		DBApp dbApp = new DBApp();
		dbApp.init();
		// int x;
//		Hashtable htblColNameType = new Hashtable();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType. put("date", " java.util.Date");
//		htblColNameType.put("pass", "java.lang.Boolean");
//		htblColNameType.put("shape", "java.awt.Polygon");
//		dbApp.createTable(strTableName, "id", htblColNameType);
		// dbApp.createBIndex(strTableName, "gpa");
//		for (int y=1; y<=8; y++) {
//		for (int i = 1; i <= 3; i++) {
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//			htblColNameValue.put("id", new Integer(i));
//			htblColNameValue.put("date", new Date(Integer.parseInt("200"+i),11,11));
//			htblColNameValue.put("pass", new Boolean(true));
//			int[] x = {i,i * 2, i * 3 };
//			// int [] y = {i,i+1,i+2};
//			Polygon p = new Polygon(x, x, 3);
//			htblColNameValue.put("shape", p);
//			dbApp.insertIntoTable(strTableName, htblColNameValue);
//		}
////		 }
//System.out.println(new Date(Integer.parseInt("101"),10,11));
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(453455));
//		htblColNameValue.put("name", new String("Ahmed Noor"));
//		htblColNameValue.put("gpa", new Double(0.95));
//		x =453455;
//		tree.insert(x, new Ref(1,2));
//		System.out.println(tree.toString());
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(5674567));
//		htblColNameValue.put("name", new String("Dalia Noor"));
//		htblColNameValue.put("gpa", new Double(1.25));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		x =5674567;
//		tree.insert(x, new Ref(1,4));
//		System.out.println(tree.toString());
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(23498));
//		htblColNameValue.put("name", new String("John Noor"));
//		htblColNameValue.put("gpa", new Double(1.5));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		x =23498;
//		tree.insert(x, new Ref(1,0));
//		System.out.println(tree.toString());
//		htblColNameValue.clear();
//		htblColNameValue.put("id", new Integer(78452));
//		htblColNameValue.put("name", new String("Zaky Noor"));
//		htblColNameValue.put("gpa", new Double(0.88));
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		x =78452;
//		tree.insert(x, new Ref(1,1));
//		System.out.println(tree.toString());
////		

//		System.out.println((tree.search(5674567)).toString());

//

//	    dbApp.createRTreeIndex("Student", "shape");
//		dbApp.createBTreeIndex("Student", "gpa");
//		test2("Student-id");
		System.out.println(test2("Student-gpa").toString());
//////
//		 for (int i = 6; i <= 201; i++) {
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//            htblColNameValue.put("date", new Date(Integer.parseInt("2001"),11,11));
//            dbApp.deleteFromTable(strTableName,htblColNameValue);
          //  }
//			htblColNameValue.put("name", new String("Ahmad"));
//			int [] x = {2981,299,300};
//			int [] y = {29, 2972, 298};
//			Polygon p =new Polygon(x,y,3);
//		//	 htblColNameValue.put("shape", p);
//			 		 
//			dbApp.updateTable(strTableName , "(299,299),(300,300),(301,301)", htblColNameValue);
//		
////		test("Student");
     //	    test("Student");
		// System.out.println(test2("Student-id").toString());
		 System.out.println(test3("Student-shape").toString());
		// System.out.println(test2("Student-id").search(299));
//		int[] x= {1,2,3};
//		int [] y = {1,2,3};
//		PolygonDB p = new PolygonDB(x,y,3);
//		
//		System.out.println(test3("Student-shape").search(p.getArea()));
//	
//		for (int j = 1; j <= 300; j++) {
//			//System.out.println(j);
//			System.out.println(test2("Student-id").search(j));
//			// System.out.println(test2("Student-gpa").search(new Double(j)));
//		}
//		for (int j=0; j<=30; j++) {
//			// System.out.println(j);
//			System.out.println(test2("Student-gpa").search(new Double(j)));
//			
//		}
//		System.out.println(test2("Student-id").search(251).toString());
//		System.out.println(test2("Student-id").search(118).toString());
//		System.out.println(test2("Student-id").search(119).toString());
//		System.out.println(test2("Student-id").search(120).toString());
//		System.out.println(test2("Student-id").search(121).toString());
//		System.out.println(test2("Student-id").search(122).toString());
//		System.out.println(test2("Student-id").search(123).toString());
//		System.out.println(test2("Student-id").search(124).toString());
//			
//		System.out.println(test2("Student-id").search(125).toString());
//	
//		for(int i=0; i<10; i++)
//		{
//			tree.insert(i, new Ref(1,i+1));
//		}
//		tree.insert(10, new Ref(1,11));
//		tree.insert(10,  new Ref(1,12));
//		tree.insert(10,  new Ref(1,13));
//		System.out.println(tree.toString());
//		System.out.println(tree.search(10).toString());

//		for (int i = 1; i <= 300; i++) {
//			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
//			// htblColNameValue.put("id", new Integer(i));
//			htblColNameValue.put("name", new String("Jerry" + 1 / 10));
//			htblColNameValue.put("gpa", new Double(1 / 10));
//			int[] x = { i, i * 2, 2 };
//			int[] y = { i, i * 3, i };
//			Polygon p = new Polygon(x, y, 3);
//
//			htblColNameValue.put("shape", p);
//			// dbApp.insertIntoTable(strTableName, htblColNameValue);
//			dbApp.updateTable(strTableName, i + "", htblColNameValue);
//			// dbApp.deleteFromTable(strTableName, htblColNameValue);
//		}
		// dbApp.createRTreeIndex("Student", "shape");
		// for (int i = 6; i <= 300; i++) {
//			int[] x1 = { 1, 2, 3 };
//			int[] y1 = { 1, 2, 3 };
//			Polygon p1 = new Polygon(x1, y1, 3);
//			PolygonDB p2 = new PolygonDB(p1);
//			System.out.println(p2.getArea());
//			System.out.println(test3("Student-shape").search(p2.getArea()));
////			
////			
//			PolygonDB p3 = new PolygonDB(p);
//			System.out.println(p3.getArea());
//			System.out.println(test3("Student-shape").search(p3.getArea()));
////

//		}
//		test("Student");
//     dbApp.createBTreeIndex("Student", "id");
//     dbApp.createBTreeIndex("Student", "name");
//     dbApp.createBTreeIndex("Student", "gpa");
//		System.out.println(test3("Student-shape").toString());
//
		int[] x1 = { 200, 200*2, 200*3 };
		int[] y1 = { 1, 2, 3 };
		Polygon p1 = new Polygon(x1, x1, 3);
//
//		System.out.println(test3("Student-shape").search(p1.getArea()));
//		PolygonDB p2=new PolygonDB(p);
//		System.out.println(test3("Student-shape").search(p2.getArea()));
		// System.out.println(p2.getArea());

//		String strTableName = "Student";
//		DBApp dbApp = new DBApp( );
//		System.out.println(test2("Student-id"));
		// System.out.println(test2("Student-gpa"));
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[1];
		arrSQLTerms[0] = new SQLTerm();
//		arrSQLTerms[1] = new SQLTerm();
//		arrSQLTerms[2] = new SQLTerm();
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName = "shape";
		arrSQLTerms[0]._strOperator = "!=";
		arrSQLTerms[0]._objValue =p1;
//		arrSQLTerms[1]._strTableName = "Student";
//		arrSQLTerms[1]._strColumnName = "name";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = "Ahmad0";
//		arrSQLTerms[2]._strTableName = "Student";
//		arrSQLTerms[2]._strColumnName = "id";
//		arrSQLTerms[2]._strOperator = ">";
//		arrSQLTerms[2]._objValue = new Integer(299);
		String[] strarrOperators = new String[0];
//		strarrOperators[0] = "OR";
//		strarrOperators[1] = "OR";
//		// select * from Student where name = �John Noor� or gpa = 1.5;
		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
		test("Student");
		tes(strTableName);
		System.out.println(resultSet.hasNext());
		testSelect(resultSet);

	}
}
