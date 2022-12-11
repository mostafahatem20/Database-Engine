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
//			int[] x = ((Polygon) c.get("shape")).xpoints;
//			int[] y = ((Polygon) c.get("shape")).ypoints;
//			for (int p = 0; p < x.length; p++) {
//				System.out.print("x" + p + "= " + x[p] + ", ");
//				System.out.print("y" + p + "= " + y[p] + ", ");
//			}

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
//				int[] x = ((Polygon) ((Hashtable<?, ?>) vector.get(j)).get("shape")).xpoints;
//				int[] y = ((Polygon) ((Hashtable<?, ?>) vector.get(j)).get("shape")).ypoints;
//				for (int p = 0; p < x.length; p++) {
//					System.out.print("x" + p + "= " + x[p] + ", ");
//					System.out.print("y" + p + "= " + y[p] + ", ");
//				}

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
		String strTableName = "Student";
		DBApp dbApp = new DBApp( );
		dbApp.init();
//		Hashtable htblColNameType = new Hashtable( );
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("gpa", "java.lang.Double");
//		dbApp.createTable( strTableName, "id", htblColNameType );
//		dbApp.createBTreeIndex( strTableName, "gpa" );
//		Hashtable htblColNameValue = new Hashtable( );
//		htblColNameValue.put("id", new Integer( 2343432 ));
//		htblColNameValue.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 453455 ));
//		htblColNameValue.put("name", new String("Ahmed Noor" ) );
//		htblColNameValue.put("gpa", new Double( 0.95 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 5674567 ));
//		htblColNameValue.put("name", new String("Dalia Noor" ) );
//		htblColNameValue.put("gpa", new Double( 1.25 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 23498 ));
//		htblColNameValue.put("name", new String("John Noor" ) );
//		htblColNameValue.put("gpa", new Double( 1.5 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
//		htblColNameValue.clear( );
//		htblColNameValue.put("id", new Integer( 78452 ));
//		htblColNameValue.put("name", new String("Zaky Noor" ) );
//		htblColNameValue.put("gpa", new Double( 0.88 ) );
//		dbApp.insertIntoTable( strTableName , htblColNameValue );
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[2];
		arrSQLTerms[0]= new SQLTerm();
		arrSQLTerms[1]= new SQLTerm();
		arrSQLTerms[0]._strTableName = "Student";
		arrSQLTerms[0]._strColumnName= "name";
		arrSQLTerms[0]._strOperator = "=";
		arrSQLTerms[0]._objValue = "John Noor";
		arrSQLTerms[1]._strTableName = "Student";
		arrSQLTerms[1]._strColumnName= "gpa";
		arrSQLTerms[1]._strOperator = "=";
		arrSQLTerms[1]._objValue = new Double( 1.5 );
		String[]strarrOperators = new String[1];
		strarrOperators[0] = "OR";
		// select * from Student where name = “John Noor” or gpa = 1.5;
		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators); 
		test("Student");
	
		System.out.println(resultSet.hasNext());
		testSelect(resultSet);

	}
}
