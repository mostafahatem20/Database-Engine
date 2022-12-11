package Notsosoftware;

import java.awt.List;
import java.awt.Polygon;
import java.lang.Math;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

public class DBApp implements java.io.Serializable {
	//
// create and throw DBAppException
	private int n;
	private int node;

	public void init() {
		Properties p = new Properties();
		try {
			p.load(new FileInputStream("config/DBApp.properties"));
			String number = p.getProperty("MaximumRowsCountinPage");
			n = Integer.parseInt(number);
			number = p.getProperty("NodeSize");
			node = Integer.parseInt(number);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}

		File meta = new File("data/metadata.csv");
		if (!meta.exists()) {
			try {
				meta.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		// checking if table already exists
		Boolean tableExists = false;
		File file1 = new File("data/metadata.csv");
		try {
			Scanner sc = new Scanner(file1);

			// sc.useDelimiter("[,\n]");

			String scTableName, scColName, scColType, scKey, scIndexed;
			String line = "";
			while (sc.hasNextLine() && !tableExists) {
				// System.out.println("ay 7aga");
				line = sc.nextLine();
				String[] l = line.split(",");
				if (l.length == 5) {
					scTableName = l[0];
					scColName = l[1];
					scColType = l[2];
					scKey = l[3];
					scIndexed = l[4];
					if (scTableName.equals(strTableName)) {
						// System.out.println("exists");
						tableExists = true;
					}

				}

			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		if (tableExists)
			throw new DBAppException("Table " + strTableName + " already exists");
		else {
			MetaDataTable metaTable = new MetaDataTable(strTableName, htblColNameType.size());
			MetaDataCol metaCol;
			Set<String> keySet = htblColNameType.keySet();
			int i = 0;
			for (String key : keySet) {
				if (key == strClusteringKeyColumn) {
					if(!htblColNameType.get(key).equals("java.lang.Boolean"))
					metaCol = new MetaDataCol(strTableName, key, htblColNameType.get(key), true, false);
					else
						throw new DBAppException("cannot create a table with boolean as a clustring key");
				}
				else
					metaCol = new MetaDataCol(strTableName, key, htblColNameType.get(key), false, false);
				metaTable.setMetaDataColArray(metaCol, i);
				i++;

			}
			try {
				FileWriter fw = new FileWriter("data/metadata.csv", true);
				BufferedWriter bw = new BufferedWriter(fw);
				// PrintWriter pw = new PrintWriter(bw);
				bw.write(metaTable.toString());
				bw.close();
				fw.close();

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	public String getKeyMeta(String strTableName) throws FileNotFoundException {
		Boolean tableExists = false;
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		// sc.useDelimiter("[,\n]");

		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		int counter = 0;
		while (sc.hasNextLine()) {
			String[] l = line.split(",");
			line = sc.nextLine();
			counter++;
		}
		sc = new Scanner(file1);
		line = "";
		for (int i = 0; i < counter; i++) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				scColName = l[1];
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				// System.out.println(scColName);
				if (scTableName.equals(strTableName) && scKey.toLowerCase().equals("true")) {
					// System.out.println(scColName);
					return scColName;
				}

			}

		}
		return "";
	}

	public Boolean checkMeta(String strTableName) throws FileNotFoundException {
		Boolean tableExists = false;
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		// sc.useDelimiter("[,\n]");

		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		while (sc.hasNextLine() && !tableExists) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				scColName = l[1];
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				if (scTableName.equals(strTableName)) {
					tableExists = true;
				}

			}
		}
		return tableExists;
	}

	// there is a file
	public Boolean checkFile(String strTableName) {
		File file = new File("data/" + strTableName + "1.class");
		return file.exists();
	}

	public int lastFile(String strTableName) {
		File file;
		int i = 1;
		Boolean flag = false;
		while (!flag) {
			file = new File("data/" + strTableName + i + ".class");
			if (!file.exists()) {
				flag = true;
				i--;
			} else
				i++;
		}
		return i;
	}

	public int tablesize(String strTableName) throws IOException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		int i = 0;
		String line = "";
		String scTableName;
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			scTableName = l[0];
			if (scTableName.equals(strTableName)) {
				i++;
			}
		}
		return i;
	}

	public Boolean matches(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		boolean f = true;
		// sc.useDelimiter("[,\n]");

		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		Object x = null;
		int i = tablesize(strTableName);
		if (htblColNameValue.size() - 1 == i) {
			while (sc.hasNextLine()) {
				line = sc.nextLine();
				String[] l = line.split(",");
				if (l.length == 5) {
					scTableName = l[0];
					// System.out.println(scTableName);
					scColName = l[1];
					// System.out.println(scColName);
					scColType = l[2];
					scKey = l[3];
					scIndexed = l[4];
					// System.out.println(scColName);
					if (scTableName.equals(strTableName)) {
						if (htblColNameValue.get(scColName) == null) {
							return false;
						}
						// System.out.println("1");
						//System.out.println(scColType);
						switch (scColType) {
						case ("java.lang.String"):
							if (!(htblColNameValue.get(scColName) instanceof String)) {

								f = false;

							}
							break;
						case ("java.lang.Integer"):
							if (!(htblColNameValue.get(scColName) instanceof Integer)) {

								f = false;

							}
							break;
						case ("java.lang.Double"):
							if (!(htblColNameValue.get(scColName) instanceof Double)) {

								f = false;

							}
							break;
						case ("java.awt.Polygon"):
							if (!(htblColNameValue.get(scColName) instanceof Polygon)) {
								f = false;

							}
							break;
						case ("java.lang.Boolean"):
							if (!(htblColNameValue.get(scColName) instanceof Boolean)) {
								f = false;

							}
							break;
						case ("java.util.Date"):
							//System.out.println("lol");
							if (!(htblColNameValue.get(scColName) instanceof Date)) {
								f = false;

							}
							break;
						default:
							f = false;
							break;
						}

					}

				}
			}
		} else {
	//		System.out.println(htblColNameValue.size() + " " + i);
			return false;
		}
		return f;
	}

	public Vector sorts(Vector<Hashtable<String, Object>> vector, String keyname) {
		Vector vector1 = new Vector();
		Hashtable temp = vector.remove(vector.size() - 1);
		String target = (String) temp.get(keyname);
		int l = 0;
		if (target.compareTo((String) (vector.get(vector.size() - 1).get(keyname))) > 0) {
			l = vector.size();
		}

		int r = vector.size() - 1;
		while (l < r) {
			int m = l + (r - l) / 2;
			if (target.compareTo((String) (vector.get(m).get(keyname))) > 0) {
				l = m + 1;
			} else {
				r = m;
			}
		}
		Boolean flag = false;
		for (int i = l; i < vector.size(); i++) {
			if (!((String) (vector.get(i).get(keyname))).equals(target)) {
				vector.add(i, temp);
				flag = true;
				vector1.add(i);
				break;
			}

		}
		if (flag == false) {
			vector.add(vector.size(), temp);
			vector1.add(vector.size() - 1);
		}

		vector1.add(vector);
		return vector1;
	}

	public Vector sortp(Vector<Hashtable<String, Object>> vector, String keyname) {
		Vector vector1 = new Vector();
		Hashtable temp = vector.remove(vector.size() - 1);
		Polygon target = (Polygon) temp.get(keyname);
		int l = 0;
		if ((new PolygonDB((Polygon) (vector.get(vector.size() - 1).get(keyname)))).compareTo(target) <= 0) {
			l = vector.size();
		}

		int r = vector.size() - 1;
		while (l < r) {
			int m = l + (r - l) / 2;
			if ((new PolygonDB((Polygon) (vector.get(m).get(keyname)))).compareTo(target) <= 0) {
				l = m + 1;
			} else {
				r = m;
			}
		}
		Boolean flag = false;
		for (int i = l; i < vector.size(); i++) {
			if (!((new PolygonDB((Polygon) (vector.get(i).get(keyname)))).compareTo(target) == 0)) {
				vector.add(i, temp);
				flag = true;
				vector1.add(i);
				break;
			}

		}
		if (flag == false) {
			vector.add(vector.size(), temp);
			vector1.add(vector.size() - 1);
		}

		vector1.add(vector);
		return vector1;
	}

	public Vector sorti(Vector<Hashtable<String, Object>> vector, String keyname) {
		Vector vector1 = new Vector();
		Hashtable temp = vector.remove(vector.size() - 1);
		int target = (Integer) temp.get(keyname);
		int l = 0;
		if (target > (Integer) (vector.get(vector.size() - 1).get(keyname))) {
			l = vector.size();
		}

		int r = vector.size() - 1;
		while (l < r) {
			int m = l + (r - l) / 2;
			if (target > (Integer) (vector.get(m).get(keyname))) {
				l = m + 1;
			} else {
				r = m;
			}
		}
		Boolean flag = false;
		for (int i = l; i < vector.size(); i++) {
			if (!(((Integer) (vector.get(i).get(keyname))).compareTo(target) == 0)) {
				vector.add(i, temp);
				flag = true;
				vector1.add(i);
				break;
			}

		}
		if (flag == false) {
			vector.add(vector.size(), temp);
			vector1.add(vector.size() - 1);
		}

		vector1.add(vector);
		return vector1;

	}

	public Vector sortDate(Vector<Hashtable<String, Object>> vector, String keyname) {
		Vector vector1 = new Vector();
		Hashtable temp = vector.remove(vector.size() - 1);
		Date target = (Date) temp.get(keyname);
		int l = 0;
		if (target.compareTo((Date) (vector.get(vector.size() - 1).get(keyname))) > 0) {
			l = vector.size();
		}

		int r = vector.size() - 1;
		while (l < r) {
			int m = l + (r - l) / 2;
			if (target.compareTo((Date) (vector.get(m).get(keyname))) > 0) {
				l = m + 1;
			} else {
				r = m;
			}
		}
		Boolean flag = false;
		for (int i = l; i < vector.size(); i++) {
			if (!(((Date) (vector.get(i).get(keyname))).compareTo(target) == 0)) {
				vector.add(i, temp);
				vector1.add(i);
				break;
			}

		}
		if (flag == false) {
			vector.add(vector.size(), temp);
			vector1.add(vector.size() - 1);
		}

		vector1.add(vector);
		return vector1;
	}

	public Vector sortDouble(Vector<Hashtable<String, Object>> vector, String keyname) {
		Vector vector1 = new Vector();
		Hashtable temp = vector.remove(vector.size() - 1);
		Double target = (Double) temp.get(keyname);
		int l = 0;
		if (target.compareTo((Double) (vector.get(vector.size() - 1).get(keyname))) > 0) {
			l = vector.size();
		}

		int r = vector.size() - 1;
		while (l < r) {
			int m = l + (r - l) / 2;
			if (target.compareTo((Double) (vector.get(m).get(keyname))) > 0) {
				l = m + 1;
			} else {
				r = m;
			}
		}
		Boolean flag = false;
		for (int i = l; i < vector.size(); i++) {
			if (!(((Double) (vector.get(i).get(keyname))).compareTo(target) == 0)) {
				vector.add(i, temp);
				vector1.add(i);
				break;
			}

		}
		if (flag == false) {
			vector.add(vector.size(), temp);
			vector1.add(vector.size() - 1);
		}

		vector1.add(vector);
		return vector1;
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		// check type of inputs
		String lastFile;
		Vector<Hashtable<String, Object>> vector;
		Vector<Hashtable<String, Object>> vector2;
		int fileNum = 1;
		Hashtable temp = new Hashtable();
		Boolean inserted = false;
		Boolean reinsert = false;
		int IndexInPage = 0;
		int PageNum = 0;
		if (!htblColNameValue.containsKey("TouchDate")) {
			Date Touchdate = Calendar.getInstance().getTime();
			htblColNameValue.put("TouchDate", Touchdate);
		}

		try {
			if (checkMeta(strTableName)) {
				if (matches(strTableName, htblColNameValue)) {
					if (checkFile(strTableName)) {
						String s = getKeyMeta(strTableName);
						if (!checkIndexed(strTableName, s)) {

							// System.out.println("LLLLL");
							for (int i = 1; i <= lastFile(strTableName); i++) {

								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
								Object key = vector.get(vector.size() - 1).get(getKeyMeta(strTableName));
								if (key instanceof String) {

									String k = (String) key;
									if (k.compareTo((String) (htblColNameValue.get(getKeyMeta(strTableName)))) > 0) {
										inserted = true;
										if (vector.size() == n) {
											temp = vector.get(vector.size() - 1);
											vector.remove(vector.size() - 1);
											vector.add(htblColNameValue);
											Vector v2 = sorts(vector, getKeyMeta(strTableName));
											vector = (Vector<Hashtable<String, Object>>) v2.get(1);
											IndexInPage = (int) v2.get(0);
											reinsert = true;

										} else {
											vector.add(htblColNameValue);
											Vector v2 = sorts(vector, getKeyMeta(strTableName));
											vector = (Vector) v2.get(1);
											IndexInPage = (int) v2.get(0);
										}

									} else if (vector.size() < n && lastFile(strTableName) == i) {
										IndexInPage = vector.size();
										vector.add(htblColNameValue);
										inserted = true;
									}
								} else if (key instanceof Integer) {

									Integer k = (Integer) key;
									if (k.compareTo((Integer) (htblColNameValue.get(getKeyMeta(strTableName)))) > 0) {
										inserted = true;
										if (vector.size() == n) {
											temp = vector.get(vector.size() - 1);
											vector.remove(vector.size() - 1);
											vector.add(htblColNameValue);
											Vector v2 = sorti(vector, getKeyMeta(strTableName));
											vector = (Vector<Hashtable<String, Object>>) v2.get(1);
											IndexInPage = (int) v2.get(0);
											reinsert = true;

										} else {
											vector.add(htblColNameValue);
											Vector v2 = sorti(vector, getKeyMeta(strTableName));
											vector = (Vector) v2.get(1);
											IndexInPage = (int) v2.get(0);
										}

									} else if (vector.size() < n && lastFile(strTableName) == i) {
										IndexInPage = vector.size();
										vector.add(htblColNameValue);
										inserted = true;
									}
								} else if (key instanceof Date) {

									Date k = (Date) key;
									if (k.compareTo((Date) (htblColNameValue.get(getKeyMeta(strTableName)))) > 0) {
										inserted = true;
										if (vector.size() == n) {
											temp = vector.get(vector.size() - 1);
											vector.remove(vector.size() - 1);
											vector.add(htblColNameValue);
											Vector v2 = sortDate(vector, getKeyMeta(strTableName));
											vector = (Vector<Hashtable<String, Object>>) v2.get(1);
											IndexInPage = (int) v2.get(0);
											reinsert = true;

										} else {
											vector.add(htblColNameValue);
											Vector v2 = sortDate(vector, getKeyMeta(strTableName));
											vector = (Vector) v2.get(1);
											IndexInPage = (int) v2.get(0);
										}

									} else if (vector.size() < n && lastFile(strTableName) == i) {
										IndexInPage = vector.size();
										vector.add(htblColNameValue);
										inserted = true;
									}
								} else if (key instanceof Double) {

									Double k = (Double) key;
									if (k.compareTo((Double) (htblColNameValue.get(getKeyMeta(strTableName)))) > 0) {
										inserted = true;
										if (vector.size() == n) {
											temp = vector.get(vector.size() - 1);
											vector.remove(vector.size() - 1);
											vector.add(htblColNameValue);
											Vector v2 = sortDouble(vector, getKeyMeta(strTableName));
											vector = (Vector<Hashtable<String, Object>>) v2.get(1);
											IndexInPage = (int) v2.get(0);
											reinsert = true;

										} else {
											vector.add(htblColNameValue);
											Vector v2 = sortDouble(vector, getKeyMeta(strTableName));
											vector = (Vector) v2.get(1);
											IndexInPage = (int) v2.get(0);
										}

									} else if (vector.size() < n && lastFile(strTableName) == i) {
										IndexInPage = vector.size();
										vector.add(htblColNameValue);
										inserted = true;
									}
								} else if (key instanceof Polygon) {
									PolygonDB k = new PolygonDB((Polygon) key);
									if (k.compareTo((Polygon) (htblColNameValue.get(getKeyMeta(strTableName)))) > 0) {
										inserted = true;
										if (vector.size() == n) {
											temp = vector.get(vector.size() - 1);
											vector.remove(vector.size() - 1);
											vector.add(htblColNameValue);
											Vector v2 = sortp(vector, getKeyMeta(strTableName));
											vector = (Vector<Hashtable<String, Object>>) v2.get(1);
											IndexInPage = (int) v2.get(0);
											reinsert = true;

										} else {
											vector.add(htblColNameValue);
											Vector v2 = sortp(vector, getKeyMeta(strTableName));
											vector = (Vector) v2.get(1);
											IndexInPage = (int) v2.get(0);
										}

									} else if (vector.size() < n && lastFile(strTableName) == i) {
										IndexInPage = vector.size();
										vector.add(htblColNameValue);
										inserted = true;
									}
								}
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
								} catch (IOException e) {
									e.printStackTrace();
								}

								if (inserted) {
									PageNum = i;
									break;
								}

							}

							if (!inserted) {
								PageNum = lastFile(strTableName) + 1;
								IndexInPage = 0;
								vector2 = new Vector();
								vector2.add(htblColNameValue);
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + (lastFile(strTableName) + 1) + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector2);
									out.close();
									fileOut.close();
									// System.out.println("Serialized data is saved");
								} catch (IOException e) {
									e.printStackTrace();
								}
							}

						} else {
							BPTree tree = new BPTree(node);
							RTree tree1 = new RTree(node);
							try {
								FileInputStream fileIn = new FileInputStream(
										"data/" + strTableName + "-" + s + ".class");
								ObjectInputStream in = new ObjectInputStream(fileIn);
								if (htblColNameValue.get(s) instanceof Polygon)
									tree1 = (RTree) in.readObject();
								else
									tree = (BPTree) in.readObject();
								in.close();
								fileIn.close();
							} catch (IOException e) {
								e.printStackTrace();
								return;
							}
							Vector r;
							Vector<Ref> v;
							if (htblColNameValue.get(s) instanceof String) {
								r = tree.searchPos((String) htblColNameValue.get(s));
								PageNum = ((Ref) r.get(1)).getPage();
								IndexInPage = ((Ref) r.get(1)).getIndexInPage();
								if ((boolean) r.get(0))
									IndexInPage = ((Ref) r.get(1)).getIndexInPage() + 1;
								if (IndexInPage >= n) {
									PageNum += 1;
									IndexInPage = 0;
								}

								Ref r2 = new Ref(PageNum, IndexInPage);
								File filenew = new File("data/" + strTableName + PageNum + ".class");
								if (filenew.exists()) {

									try {
										FileInputStream fileIn = new FileInputStream(
												"data/" + strTableName + PageNum + ".class");

										ObjectInputStream in = new ObjectInputStream(fileIn);
										vector = (Vector) in.readObject();
										in.close();
										fileIn.close();
									} catch (IOException e) {
										e.printStackTrace();
										return;
									}
								} else {
									vector = new Vector();
								}
								Vector v3 = tree.search((String) (htblColNameValue.get(s)));
								if (v3 == null) {
									v3 = new Vector<Ref>();
									v3.add(new Ref(PageNum, IndexInPage));
									tree.insert((String) (htblColNameValue.get(s)), v3);
								} else if (v3.size() >= 1) {
									v3.add(new Ref(PageNum, IndexInPage));
									tree.update((String) (htblColNameValue.get(s)), v3);
								}

								// System.out.println("LOLOLO");
								tree.update2((String) (htblColNameValue.get(s)), r2, n);

								if (vector.size() < n) {
									vector.add(IndexInPage, htblColNameValue);

								} else {
									temp = vector.remove(vector.size() - 1);
									tree.delete((String) (temp.get(s)), PageNum, n - 1);
									vector.add(IndexInPage, htblColNameValue);
									reinsert = true;
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + PageNum + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + "-" + s + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(tree);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							} else if (htblColNameValue.get(s) instanceof Integer) {

								r = tree.searchPos((Integer) htblColNameValue.get(s));

								// System.out.println(r.toString());

								PageNum = ((Ref) r.get(1)).getPage();
								IndexInPage = ((Ref) r.get(1)).getIndexInPage();
								if ((boolean) r.get(0))
									IndexInPage = ((Ref) r.get(1)).getIndexInPage() + 1;
								if (IndexInPage >= n) {
									PageNum += 1;
									IndexInPage = 0;
								}

								Ref r2 = new Ref(PageNum, IndexInPage);
								File filenew = new File("data/" + strTableName + PageNum + ".class");
								if (filenew.exists()) {

									try {
										FileInputStream fileIn = new FileInputStream(
												"data/" + strTableName + PageNum + ".class");

										ObjectInputStream in = new ObjectInputStream(fileIn);
										vector = (Vector) in.readObject();
										in.close();
										fileIn.close();
									} catch (IOException e) {
										e.printStackTrace();
										return;
									}
								} else {
									vector = new Vector();
								}
								Vector v3 = tree.search((Integer) (htblColNameValue.get(s)));
								if (v3 == null) {
									v3 = new Vector<Ref>();
									v3.add(new Ref(PageNum, IndexInPage));
									tree.insert((Integer) (htblColNameValue.get(s)), v3);
								} else if (v3.size() >= 1) {
									v3.add(new Ref(PageNum, IndexInPage));
									tree.update((Integer) (htblColNameValue.get(s)), v3);
								}

								// System.out.println("LOLOLO");
								tree.update2((Integer) (htblColNameValue.get(s)), r2, n);

								if (vector.size() < n) {
									vector.add(IndexInPage, htblColNameValue);

								} else {
									temp = vector.remove(vector.size() - 1);
									tree.delete((Integer) (temp.get(s)), PageNum, n - 1);
									vector.add(IndexInPage, htblColNameValue);
									reinsert = true;
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + PageNum + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + "-" + s + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(tree);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							} else if (htblColNameValue.get(s) instanceof Double) {
								r = tree.searchPos((Double) htblColNameValue.get(s));
								PageNum = ((Ref) r.get(1)).getPage();
								IndexInPage = ((Ref) r.get(1)).getIndexInPage();
								if ((boolean) r.get(0))
									IndexInPage = ((Ref) r.get(1)).getIndexInPage() + 1;
								if (IndexInPage >= n) {
									PageNum += 1;
									IndexInPage = 0;
								}

								Ref r2 = new Ref(PageNum, IndexInPage);
								File filenew = new File("data/" + strTableName + PageNum + ".class");
								if (filenew.exists()) {

									try {
										FileInputStream fileIn = new FileInputStream(
												"data/" + strTableName + PageNum + ".class");

										ObjectInputStream in = new ObjectInputStream(fileIn);
										vector = (Vector) in.readObject();
										in.close();
										fileIn.close();
									} catch (IOException e) {
										e.printStackTrace();
										return;
									}
								} else {
									vector = new Vector();
								}
								Vector v3 = tree.search((Double) (htblColNameValue.get(s)));
								if (v3 == null) {
									v3 = new Vector<Ref>();
									v3.add(new Ref(PageNum, IndexInPage));
									tree.insert((Double) (htblColNameValue.get(s)), v3);
								} else if (v3.size() >= 1) {
									v3.add(new Ref(PageNum, IndexInPage));
									tree.update((Double) (htblColNameValue.get(s)), v3);
								}

								// System.out.println("LOLOLO");
								tree.update2((Double) (htblColNameValue.get(s)), r2, n);

								if (vector.size() < n) {
									vector.add(IndexInPage, htblColNameValue);

								} else {
									temp = vector.remove(vector.size() - 1);
									tree.delete((Double) (temp.get(s)), PageNum, n - 1);
									vector.add(IndexInPage, htblColNameValue);
									reinsert = true;
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + PageNum + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + "-" + s + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(tree);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							} else if (htblColNameValue.get(s) instanceof Polygon) {

								r = tree1.searchPos(
										(Double) (new PolygonDB((Polygon) htblColNameValue.get(s))).getArea());
								PageNum = ((Ref) r.get(1)).getPage();
								IndexInPage = ((Ref) r.get(1)).getIndexInPage();
								if ((boolean) r.get(0))
									IndexInPage = ((Ref) r.get(1)).getIndexInPage() + 1;
								if (IndexInPage >= n) {
									PageNum += 1;
									IndexInPage = 0;
								}

								Ref r2 = new Ref(PageNum, IndexInPage);
								File filenew = new File("data/" + strTableName + PageNum + ".class");
								if (filenew.exists()) {

									try {
										FileInputStream fileIn = new FileInputStream(
												"data/" + strTableName + PageNum + ".class");

										ObjectInputStream in = new ObjectInputStream(fileIn);
										vector = (Vector) in.readObject();
										in.close();
										fileIn.close();
									} catch (IOException e) {
										e.printStackTrace();
										return;
									}
								} else {
									vector = new Vector();
								}
								Vector v3 = tree1
										.search((Double) (new PolygonDB((Polygon) htblColNameValue.get(s))).getArea());
								if (v3 == null) {
									v3 = new Vector<Ref>();
									v3.add(new Ref(PageNum, IndexInPage));
									tree1.insert((Double) (new PolygonDB((Polygon) htblColNameValue.get(s))).getArea(),
											v3);
								} else if (v3.size() >= 1) {
									v3.add(new Ref(PageNum, IndexInPage));
									tree1.update((Double) (new PolygonDB((Polygon) htblColNameValue.get(s))).getArea(),
											v3);
								}

								// System.out.println("LOLOLO");
								tree1.update2((Double) (new PolygonDB((Polygon) htblColNameValue.get(s))).getArea(), r2,
										n);

								if (vector.size() < n) {
									vector.add(IndexInPage, htblColNameValue);

								} else {
									temp = vector.remove(vector.size() - 1);
									tree1.delete((Double) (new PolygonDB((Polygon) htblColNameValue.get(s))).getArea(),
											PageNum, n - 1);
									vector.add(IndexInPage, htblColNameValue);
									reinsert = true;
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + PageNum + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + "-" + s + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(tree1);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							} else if (htblColNameValue.get(s) instanceof Date) {
								r = tree.searchPos((Date) htblColNameValue.get(s));
								PageNum = ((Ref) r.get(1)).getPage();
								IndexInPage = ((Ref) r.get(1)).getIndexInPage();
								if ((boolean) r.get(0))
									IndexInPage = ((Ref) r.get(1)).getIndexInPage() + 1;
								if (IndexInPage >= n) {
									PageNum += 1;
									IndexInPage = 0;
								}

								Ref r2 = new Ref(PageNum, IndexInPage);
								File filenew = new File("data/" + strTableName + PageNum + ".class");
								if (filenew.exists()) {

									try {
										FileInputStream fileIn = new FileInputStream(
												"data/" + strTableName + PageNum + ".class");

										ObjectInputStream in = new ObjectInputStream(fileIn);
										vector = (Vector) in.readObject();
										in.close();
										fileIn.close();
									} catch (IOException e) {
										e.printStackTrace();
										return;
									}
								} else {
									vector = new Vector();
								}
								Vector v3 = tree.search((Date) (htblColNameValue.get(s)));
								if (v3 == null) {
									v3 = new Vector<Ref>();
									v3.add(new Ref(PageNum, IndexInPage));
									tree.insert((Date) (htblColNameValue.get(s)), v3);
								} else if (v3.size() >= 1) {
									v3.add(new Ref(PageNum, IndexInPage));
									tree.update((Date) (htblColNameValue.get(s)), v3);
								}

								// System.out.println("LOLOLO");
								tree.update2((Date) (htblColNameValue.get(s)), r2, n);

								if (vector.size() < n) {
									vector.add(IndexInPage, htblColNameValue);

								} else {
									temp = vector.remove(vector.size() - 1);
									tree.delete((Date) (temp.get(s)), PageNum, n - 1);
									vector.add(IndexInPage, htblColNameValue);
									reinsert = true;
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + PageNum + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + "-" + s + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(tree);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							}

						}
						Vector<String> cols = IndexedMeta(strTableName);

						for (int i = 0; i < cols.size(); i++) {
							if (!cols.get(i).equals(getKeyMeta(strTableName))) {
								BPTree tree = new BPTree(node);
								RTree tree1 = new RTree(node);
								Vector r;
								try {
									FileInputStream fileIn = new FileInputStream(
											"data/" + strTableName + "-" + cols.get(i) + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									if (htblColNameValue.get(cols.get(i)) instanceof Polygon)
										tree1 = (RTree) in.readObject();
									else
										tree = (BPTree) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}

								if (htblColNameValue.get(cols.get(i)) instanceof String) {
									Ref r2 = new Ref(PageNum, IndexInPage);
									// System.out.println(r2.toString());
									if (reinsert)
										tree.delete((String) (temp.get(cols.get(i))), PageNum, n - 1);
									tree.update3((String) (htblColNameValue.get(cols.get(i))), r2, n);
									Vector<Ref> v3 = tree.search((String) (htblColNameValue.get(cols.get(i))));
									// System.out.println(v3);
									if (v3 == null) {
										v3 = new Vector<Ref>();
										v3.add(new Ref(PageNum, IndexInPage));
										tree.insert((String) (htblColNameValue.get(cols.get(i))), v3);
									} else if (v3.size() >= 1) {
										int j;
										for (j = 0; j < v3.size(); j++) {
											if (v3.get(j).getPage() > PageNum)
												break;
											else if (v3.get(j).getPage() == PageNum
													&& v3.get(j).getIndexInPage() > IndexInPage)
												break;
										}

										v3.add(j, new Ref(PageNum, IndexInPage));
										tree.update((String) (htblColNameValue.get(cols.get(i))), v3);
									}

									// System.out.println(v3.toString());
								} else if (htblColNameValue.get(cols.get(i)) instanceof Integer) {
									Ref r2 = new Ref(PageNum, IndexInPage);
									// System.out.println(r2.toString());
									if (reinsert)
										tree.delete((Integer) (temp.get(cols.get(i))), PageNum, n - 1);
									tree.update3((Integer) (htblColNameValue.get(cols.get(i))), r2, n);
									Vector<Ref> v3 = tree.search((Integer) (htblColNameValue.get(cols.get(i))));
									// System.out.println(v3);
									if (v3 == null) {
										v3 = new Vector<Ref>();
										v3.add(new Ref(PageNum, IndexInPage));
										tree.insert((Integer) (htblColNameValue.get(cols.get(i))), v3);
									} else if (v3.size() >= 1) {
										int j;
										for (j = 0; j < v3.size(); j++) {
											if (v3.get(j).getPage() > PageNum)
												break;
											else if (v3.get(j).getPage() == PageNum
													&& v3.get(j).getIndexInPage() > IndexInPage)
												break;
										}

										v3.add(j, new Ref(PageNum, IndexInPage));
										tree.update((Integer) (htblColNameValue.get(cols.get(i))), v3);
									}

									// System.out.println(v3.toString());

								} else if (htblColNameValue.get(cols.get(i)) instanceof Double) {
									Ref r2 = new Ref(PageNum, IndexInPage);
									// System.out.println(r2.toString());
									if (reinsert)
										tree.delete((Double) (temp.get(cols.get(i))), PageNum, n - 1);
									tree.update3((Double) (htblColNameValue.get(cols.get(i))), r2, n);
									Vector<Ref> v3 = tree.search((Double) (htblColNameValue.get(cols.get(i))));
									// System.out.println(v3);
									if (v3 == null) {
										v3 = new Vector<Ref>();
										v3.add(new Ref(PageNum, IndexInPage));
										tree.insert((Double) (htblColNameValue.get(cols.get(i))), v3);
									} else if (v3.size() >= 1) {
										int j;
										for (j = 0; j < v3.size(); j++) {
											if (v3.get(j).getPage() > PageNum)
												break;
											else if (v3.get(j).getPage() == PageNum
													&& v3.get(j).getIndexInPage() > IndexInPage)
												break;
										}

										v3.add(j, new Ref(PageNum, IndexInPage));
										tree.update((Double) (htblColNameValue.get(cols.get(i))), v3);
									}

									// System.out.println(v3.toString());

								} else if (htblColNameValue.get(cols.get(i)) instanceof Polygon) {
									Ref r2 = new Ref(PageNum, IndexInPage);
									// System.out.println(r2.toString());
									if (reinsert)
										tree1.delete(
												(Double) (new PolygonDB((Polygon) (temp.get(cols.get(i)))).getArea()),
												PageNum, n - 1);
									tree1.update3((Double) (new PolygonDB((Polygon) (htblColNameValue.get(cols.get(i))))
											.getArea()), r2, n);
									Vector<Ref> v3 = tree1.search(
											(Double) (new PolygonDB((Polygon) (htblColNameValue.get(cols.get(i))))
													.getArea()));

									if (v3 == null) {
										v3 = new Vector<Ref>();
										v3.add(new Ref(PageNum, IndexInPage));
										tree1.insert(
												(Double) (new PolygonDB((Polygon) (htblColNameValue.get(cols.get(i))))
														.getArea()),
												v3);
									} else if (v3.size() >= 1) {
										int j;
										for (j = 0; j < v3.size(); j++) {
											if (v3.get(j).getPage() > PageNum)
												break;
											else if (v3.get(j).getPage() == PageNum
													&& v3.get(j).getIndexInPage() > IndexInPage)
												break;
										}
										v3.add(new Ref(PageNum, IndexInPage));
										tree1.update(
												(Double) (new PolygonDB((Polygon) (htblColNameValue.get(cols.get(i))))
														.getArea()),
												v3);
									}
									// System.out.println("this" + v3.toString());
									// System.out.println("LOLOLO");

								} else if (htblColNameValue.get(cols.get(i)) instanceof Date) {
									Ref r2 = new Ref(PageNum, IndexInPage);
									// System.out.println(r2.toString());
									if (reinsert)
										tree.delete((Date) (temp.get(cols.get(i))), PageNum, n - 1);
									tree.update3((Date) (htblColNameValue.get(cols.get(i))), r2, n);
									Vector<Ref> v3 = tree.search((Date) (htblColNameValue.get(cols.get(i))));
									// System.out.println(v3);
									if (v3 == null) {
										v3 = new Vector<Ref>();
										v3.add(new Ref(PageNum, IndexInPage));
										tree.insert((Date) (htblColNameValue.get(cols.get(i))), v3);
									} else if (v3.size() >= 1) {
										int j;
										for (j = 0; j < v3.size(); j++) {
											if (v3.get(j).getPage() > PageNum)
												break;
											else if (v3.get(j).getPage() == PageNum
													&& v3.get(j).getIndexInPage() > IndexInPage)
												break;
										}

										v3.add(j, new Ref(PageNum, IndexInPage));
										tree.update((Date) (htblColNameValue.get(cols.get(i))), v3);
									}

									// System.out.println(v3.toString());

								}
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + "-" + cols.get(i) + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									if (htblColNameValue.get(cols.get(i)) instanceof Polygon)
										out.writeObject(tree1);
									else
										out.writeObject(tree);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							}

						}
						if (reinsert == true) {
							insertIntoTable(strTableName, temp);
						}

					} else {
						vector2 = new Vector();
						vector2.add(htblColNameValue);
						try {
							FileOutputStream fileOut = new FileOutputStream("data/" + strTableName + 1 + ".class");
							ObjectOutputStream out = new ObjectOutputStream(fileOut);
							out.writeObject(vector2);
							out.close();
							fileOut.close();
					//		System.out.println("first");
							Vector<String> v = IndexedMeta(strTableName);
							for (int i = 0; i < v.size(); i++) {
								BPTree tree = new BPTree(node);
								RTree tree1 = new RTree(node);
								Vector<Ref> v2 = new Vector<Ref>();
								v2.add(new Ref(1, 0));
								Boolean flag = false;

								if (htblColNameValue.get(v.get(i)) instanceof String) {
									tree.insert((String) htblColNameValue.get(v.get(i)), v2);

								} else if (htblColNameValue.get(v.get(i)) instanceof Integer) {

									tree.insert((Integer) htblColNameValue.get(v.get(i)), v2);

								} else if (htblColNameValue.get(v.get(i)) instanceof Double) {
									tree.insert((Double) htblColNameValue.get(v.get(i)), v2);

								} else if (htblColNameValue.get(v.get(i)) instanceof Polygon) {
									tree1.insert((Double) (new PolygonDB((Polygon) (htblColNameValue.get(v.get(i))))
											.getArea()), v2);
									flag = true;

								} else if (htblColNameValue.get(v.get(i)) instanceof Date) {

									tree.insert((Date) htblColNameValue.get(v.get(i)), v2);

								}

								try {
									FileOutputStream fileOut1 = new FileOutputStream(
											"data/" + strTableName + "-" + v.get(i) + ".class");
									ObjectOutputStream out1 = new ObjectOutputStream(fileOut1);
									if (!flag)
										out1.writeObject(tree);
									else {
										out1.writeObject(tree1);
									}
									out1.close();
									fileOut1.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							}

						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				} else {
					throw new DBAppException("record not valid ");
				}
			} else {
				throw new DBAppException("Table Name not valid");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (DBAppException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public Vector<String> IndexedMeta(String strTableName) throws FileNotFoundException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		Vector<String> cols = new Vector<String>();
		String f = "";
		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				scColName = l[1];
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				if (scTableName.equals(strTableName) && scIndexed.toLowerCase().equals("true")) {
					cols.add(scColName);
				}
			}
		}
		return cols;
	}

	public Boolean checkIndexed(String strTableName, String colName) throws FileNotFoundException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		String f = "";
		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				scColName = l[1];
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				if (scTableName.equals(strTableName) && scIndexed.toLowerCase().equals("true")
						&& scColName.equals(colName)) {
					return true;
				}
			}
		}
		return false;
	}

	public String getType(String strTableName) throws FileNotFoundException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		String f = "";
		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				// System.out.println(scTableName);
				scColName = l[1];
				// System.out.println(scColName);
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				// System.out.println(scColName);
				if (scTableName.equals(strTableName) && scKey.toLowerCase().equals("true")) {
					f = scColType;
				}
			}
		}
		return f;
	}

	public String getName(String strTableName) throws FileNotFoundException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		String f = "";
		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				// System.out.println(scTableName);
				scColName = l[1];
				// System.out.println(scColName);
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				// System.out.println(scColName);
				if (scTableName.equals(strTableName) && scKey.toLowerCase().equals("true")) {
					f = scColName;
				}
			}
		}
		return f;
	}

	public int[] binarySearch(Vector<Hashtable<String, Object>> v, String strClusteringKey, String Name) {
		int l = 0, r = v.size() - 1;
		int[] re = new int[2];
		re[0] = -1;
		int output = -1;
		while (l <= r) {
			int m = (l + r) / 2;
			int res = ((String) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			if (res < 0)
				l = m + 1;
			else if (res > 0)
				r = m - 1;
			else {
				// System.out.println(s+":"+m+":"+r);
				int low = lo(v, strClusteringKey, Name, m, r);
				int foo = fo(v, strClusteringKey, Name, m, l);
				re[0] = foo;
				re[1] = low;
				return re;
			}
		}
		return re;
	}

	public int lo(Vector<Hashtable<String, Object>> v, String strClusteringKey, String Name, int m, int r) {
		int l = m;
		int res = ((String) ((v.get(r)).get(Name))).compareTo(strClusteringKey);
		if (res == 0)
			return r;
		while (l <= r) {
			m = (l + r) / 2;
			int reso = ((String) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			int reso1 = ((String) ((v.get(m - 1)).get(Name))).compareTo(strClusteringKey);
			if (reso > 0 && reso1 == 0)
				return m - 1;
			else if (reso == 0)
				l = m + 1;
			else if (reso > 0 && reso1 != 0)
				r = m - 1;
		}
		return -1;
	}

	public int fo(Vector<Hashtable<String, Object>> v, String strClusteringKey, String Name, int m, int l) {
		int h = m - 1;
		int res = ((String) ((v.get(l)).get(Name))).compareTo(strClusteringKey);
		if (res == 0)
			return l;
		while (l <= h) {
			m = (l + h) / 2;
			int reso = ((String) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			int reso1 = ((String) ((v.get(m + 1)).get(Name))).compareTo(strClusteringKey);
			if (reso < 0 && reso1 == 0)
				return m + 1;
			else if (reso == 0)
				h = m - 1;
			else if (reso < 0 && reso1 != 0)
				l = m + 1;

		}
		return -1;
	}

	public int[] binarySearch(Vector<Hashtable<String, Object>> v, int strClusteringKey, String Name) {
		int l = 0, r = v.size() - 1;
		int[] re = new int[2];
		re[0] = -1;
		int output = -1;
		while (l <= r) {
			int m = (l + r) / 2;
			int res = ((Integer) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			if (res < 0)
				l = m + 1;
			else if (res > 0)
				r = m - 1;
			else {
				// System.out.println(s+":"+m+":"+r);
				if (m > 0) {
					int low = lo(v, strClusteringKey, Name, m, r);
					re[1] = low;
				}
				int foo = fo(v, strClusteringKey, Name, m, l);
				re[0] = foo;

				return re;
			}
		}
		return re;
	}

	public int lo(Vector<Hashtable<String, Object>> v, int strClusteringKey, String Name, int m, int r) {
		int l = m;
		int res = ((Integer) ((v.get(r)).get(Name))).compareTo(strClusteringKey);
		if (res == 0)
			return r;
		while (l <= r) {
			m = (l + r) / 2;
		//	System.out.println(m);
			int reso = ((Integer) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			int reso1 = ((Integer) ((v.get(m - 1)).get(Name))).compareTo(strClusteringKey);
			if (reso > 0 && reso1 == 0)
				return m - 1;
			else if (reso == 0)
				l = m + 1;
			else if (reso > 0 && reso1 != 0)
				r = m - 1;
		}
		return -1;
	}

	public int fo(Vector<Hashtable<String, Object>> v, int strClusteringKey, String Name, int m, int l) {
		int h = m - 1;
		int res = ((Integer) ((v.get(l)).get(Name))).compareTo(strClusteringKey);
		if (res == 0)
			return l;
		while (l <= h) {
			m = (l + h) / 2;
			int reso = ((Integer) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			int reso1 = ((Integer) ((v.get(m + 1)).get(Name))).compareTo(strClusteringKey);
			if (reso < 0 && reso1 == 0)
				return m + 1;
			else if (reso == 0)
				h = m - 1;
			else if (reso < 0 && reso1 != 0)
				l = m + 1;

		}
		return -1;
	}

	public int[] binarySearch(Vector<Hashtable<String, Object>> v, Double strClusteringKey, String Name) {
		int l = 0, r = v.size() - 1;
		int[] re = new int[2];
		re[0] = -1;
		int output = -1;
		while (l <= r) {
			int m = (l + r) / 2;
			int res = ((Double) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			if (res < 0)
				l = m + 1;
			else if (res > 0)
				r = m - 1;
			else {
				// System.out.println(s+":"+m+":"+r);
				int low = lo(v, strClusteringKey, Name, m, r);
				int foo = fo(v, strClusteringKey, Name, m, l);
				re[0] = foo;
				re[1] = low;
				return re;
			}
		}
		return re;
	}

	public int lo(Vector<Hashtable<String, Object>> v, Double strClusteringKey, String Name, int m, int r) {
		int l = m;
		int res = ((Double) ((v.get(r)).get(Name))).compareTo(strClusteringKey);
		if (res == 0)
			return r;
		while (l <= r) {
			m = (l + r) / 2;
			int reso = ((Double) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			int reso1 = ((Double) ((v.get(m - 1)).get(Name))).compareTo(strClusteringKey);
			if (reso > 0 && reso1 == 0)
				return m - 1;
			else if (reso == 0)
				l = m + 1;
			else if (reso > 0 && reso1 != 0)
				r = m - 1;
		}
		return -1;
	}

	public int fo(Vector<Hashtable<String, Object>> v, Double strClusteringKey, String Name, int m, int l) {
		int h = m - 1;
		int res = ((Double) ((v.get(l)).get(Name))).compareTo(strClusteringKey);
		if (res == 0)
			return l;
		while (l <= h) {
			m = (l + h) / 2;
			int reso = ((Double) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			int reso1 = ((Double) ((v.get(m + 1)).get(Name))).compareTo(strClusteringKey);
			if (reso < 0 && reso1 == 0)
				return m + 1;
			else if (reso == 0)
				h = m - 1;
			else if (reso < 0 && reso1 != 0)
				l = m + 1;

		}
		return -1;
	}

	public int[] binarySearch(Vector<Hashtable<String, Object>> v, Date strClusteringKey, String Name) {
		int l = 0, r = v.size() - 1;
		int[] re = new int[2];
		re[0] = -1;
		int output = -1;
		while (l <= r) {
			int m = (l + r) / 2;
			int res = ((Date) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			if (res < 0)
				l = m + 1;
			else if (res > 0)
				r = m - 1;
			else {
				// System.out.println(s+":"+m+":"+r);
				int low = lo(v, strClusteringKey, Name, m, r);
				int foo = fo(v, strClusteringKey, Name, m, l);
				re[0] = foo;
				re[1] = low;
				return re;
			}
		}
		return re;
	}

	public int lo(Vector<Hashtable<String, Object>> v, Date strClusteringKey, String Name, int m, int r) {
		int l = m;
		int res = ((Date) ((v.get(r)).get(Name))).compareTo(strClusteringKey);
		if (res == 0)
			return r;
		while (l <= r) {
			m = (l + r) / 2;
			int reso = ((Date) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			int reso1 = ((Date) ((v.get(m - 1)).get(Name))).compareTo(strClusteringKey);
			if (reso > 0 && reso1 == 0)
				return m - 1;
			else if (reso == 0)
				l = m + 1;
			else if (reso > 0 && reso1 != 0)
				r = m - 1;
		}
		return -1;
	}

	public int fo(Vector<Hashtable<String, Object>> v, Date strClusteringKey, String Name, int m, int l) {
		int h = m - 1;
		int res = ((Date) ((v.get(l)).get(Name))).compareTo(strClusteringKey);
		if (res == 0)
			return l;
		while (l <= h) {
			m = (l + h) / 2;
			int reso = ((Date) ((v.get(m)).get(Name))).compareTo(strClusteringKey);
			int reso1 = ((Date) ((v.get(m + 1)).get(Name))).compareTo(strClusteringKey);
			if (reso < 0 && reso1 == 0)
				return m + 1;
			else if (reso == 0)
				h = m - 1;
			else if (reso < 0 && reso1 != 0)
				l = m + 1;

		}
		return -1;
	}

	public int[] binarySearch(Vector<Hashtable<String, Object>> v, Polygon strClusteringKey, String Name) {
		int l = 0, r = v.size() - 1;
		int[] re = new int[2];
		re[0] = -1;
		int output = -1;
		while (l <= r) {
			int m = (l + r) / 2;
			int res = (new PolygonDB((Polygon) ((v.get(m)).get(Name)))).compareTo(strClusteringKey);
			if (res < 0)
				l = m + 1;
			else if (res > 0)
				r = m - 1;
			else {
				// System.out.println(s+":"+m+":"+r);
				int low = lo(v, strClusteringKey, Name, m, r);
				int foo = fo(v, strClusteringKey, Name, m, l);
				re[0] = foo;
				re[1] = low;
				return re;
			}
		}
		return re;
	}

	public int lo(Vector<Hashtable<String, Object>> v, Polygon strClusteringKey, String Name, int m, int r) {
		int l = m;
		int res = (new PolygonDB((Polygon) ((v.get(r)).get(Name)))).compareTo(strClusteringKey);
		if (res == 0)
			return r;
		while (l <= r) {
			m = (l + r) / 2;
			int reso = (new PolygonDB((Polygon) ((v.get(m)).get(Name)))).compareTo(strClusteringKey);
			int reso1 = (new PolygonDB((Polygon) ((v.get(m - 1)).get(Name)))).compareTo(strClusteringKey);
			if (reso > 0 && reso1 == 0)
				return m - 1;
			else if (reso == 0)
				l = m + 1;
			else if (reso > 0 && reso1 != 0)
				r = m - 1;
		}
		return -1;
	}

	public int fo(Vector<Hashtable<String, Object>> v, Polygon strClusteringKey, String Name, int m, int l) {
		int h = m - 1;
		int res = (new PolygonDB((Polygon) ((v.get(l)).get(Name)))).compareTo(strClusteringKey);
		if (res == 0)
			return l;
		while (l <= h) {
			m = (l + h) / 2;
			int reso = (new PolygonDB((Polygon) ((v.get(m)).get(Name)))).compareTo(strClusteringKey);
			int reso1 = (new PolygonDB((Polygon) ((v.get(m + 1)).get(Name)))).compareTo(strClusteringKey);
			if (reso < 0 && reso1 == 0)
				return m + 1;
			else if (reso == 0)
				h = m - 1;
			else if (reso < 0 && reso1 != 0)
				l = m + 1;

		}
		return -1;
	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		Vector<Hashtable<String, Object>> vector;
		Hashtable h;

		int fl = lastFile(strTableName);
		int[] index = new int[2];
		try {
			if (matchesd(strTableName, htblColNameValue)) {
				if (htblColNameValue.get(getKeyMeta(strTableName)) == null) {
					if (!checkIndexed(strTableName, getKeyMeta(strTableName))) {
						String f = getType(strTableName);
						String name = getName(strTableName);
						switch (f) {
						case ("java.lang.String"): {

							for (int i = 1; i <= lastFile(strTableName); i++) {
								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
								index = binarySearch(vector, strClusteringKey, name);
								for (int j = index[0]; j >= 0 && j <= index[1]; j++) {
									h = vector.get(j);
									Set<String> keySet = htblColNameValue.keySet();
									for (String key : keySet) {
										h.replace(key, htblColNameValue.get(key));
										h.replace("TouchDate", Calendar.getInstance().getTime());
									}
									vector.set(j, h);
									updateSec(strTableName, htblColNameValue, name, new Ref(i, j));

								}
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("Serialized data is saved");
								} catch (IOException e) {
									e.printStackTrace();
								}

								if (index[1] < n - 1 && index[0] != -1)
									break;
							}
							break;
						}
						case ("java.lang.Integer"): {
							int x = Integer.parseInt(strClusteringKey);
					//		System.out.println("yup1");
							for (int i = 1; i <= lastFile(strTableName); i++) {
					//			System.out.println("heeeeeeeh");
								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
								index = binarySearch(vector, x, name);
					//			System.out.println(i + ":" + index[0] + ":" + index[1]);
								for (int j = index[0]; j >= 0 && j <= index[1]; j++) {
									h = vector.get(j);
									Set<String> keySet = htblColNameValue.keySet();
									for (String key : keySet) {
										h.replace(key, htblColNameValue.get(key));
										h.replace("TouchDate", Calendar.getInstance().getTime());
									}
									vector.set(j, h);
									updateSec(strTableName, htblColNameValue, name, new Ref(i, j));

								}
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("Serialized data is saved");
								} catch (IOException e) {
									e.printStackTrace();
								}
								if (index[1] < n - 1 && index[0] != -1)
									break;

							}

							break;
						}
						case ("java.lang.Double"): {
							Double y = Double.parseDouble(strClusteringKey);
							for (int i = 1; i <= lastFile(strTableName); i++) {
								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
								index = binarySearch(vector, y, name);
								for (int j = index[0]; j >= 0 && j <= index[1]; j++) {
									h = vector.get(j);
									Set<String> keySet = htblColNameValue.keySet();
									for (String key : keySet) {
										h.replace(key, htblColNameValue.get(key));
										h.replace("TouchDate", Calendar.getInstance().getTime());
									}
									vector.set(j, h);
									updateSec(strTableName, htblColNameValue, name, new Ref(i, j));

								}
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("Serialized data is saved");
								} catch (IOException e) {
									e.printStackTrace();
								}
								if (index[1] < n - 1 && index[0] != -1)
									break;
							}

							break;
						}
						case ("java.awt.Polygon"): {
							StringTokenizer st = new StringTokenizer(strClusteringKey, ",");
							int n = st.countTokens();
							int z = 0;
							int yindex = 0;
							int[] x = new int[n / 2];
							int[] y = new int[n / 2];
							while (st.hasMoreTokens()) {
								String s = st.nextToken();
								if (s.charAt(0) == '(') {
									x[z] = Integer.parseInt(s.substring(1));
									z++;
								} else {
									y[yindex] = Integer.parseInt(s.substring(0, s.length() - 1));
									yindex++;
								}
							}
							Polygon po = new Polygon(x, y, n / 2);

							for (int i = 1; i <= lastFile(strTableName); i++) {
								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
								index = binarySearch(vector, po, name);

								for (int j = index[0]; j >= 0 && j <= index[1]; j++) {

									h = vector.get(j);

									if (new PolygonDB((Polygon) h.get(name)).equals(po)) {
										Set<String> keySet = htblColNameValue.keySet();

										for (String key : keySet) {
											h.replace(key, htblColNameValue.get(key));
											h.replace("TouchDate", Calendar.getInstance().getTime());
										}

										vector.set(j, h);
										updateSec(strTableName, htblColNameValue, name, new Ref(i, j));
									}
								}
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("Serialized data is saved");
								} catch (IOException e) {
									e.printStackTrace();
								}
								if (index[1] < n - 1 && index[0] != -1)
									break;
							}

							break;

						}
						case ("java.util.Date"): {
							SimpleDateFormat f1 = new SimpleDateFormat("yyyy-MM-dd");
							Date d = f1.parse(strClusteringKey);
				//			System.out.println(d);
							for (int i = 1; i <= lastFile(strTableName); i++) {
								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
								index = binarySearch(vector, d, name);
								for (int j = index[0]; j >= 0 && j <= index[1]; j++) {
									h = vector.get(j);
									Set<String> keySet = htblColNameValue.keySet();
									for (String key : keySet) {
										h.replace(key, htblColNameValue.get(key));
										h.replace("TouchDate", Calendar.getInstance().getTime());
									}
									vector.set(j, h);
									updateSec(strTableName, htblColNameValue, name, new Ref(i, j));

								}
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("Serialized data is saved");
								} catch (IOException e) {
									e.printStackTrace();
								}
								if (index[1] < n - 1 && index[0] != -1)
									break;
							}

							break;
						}
						}

					} else {
						updateIndexed(strTableName, strClusteringKey, htblColNameValue);
					}
				} else {
					throw new DBAppException("Can not update Clustering Key");

				}
			} else {
				throw new DBAppException("Record not valid.");
			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void updateIndexed(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, FileNotFoundException, ParseException {
		BPTree tree = null;
		RTree tree1 = null;
		String s1 = getKeyMeta(strTableName);
		String type = getType(strTableName);
		Vector<Ref> refer = new Vector<Ref>();
		Polygon po = null;
		Vector<Hashtable<String, Object>> page = new Vector<Hashtable<String, Object>>();
		try {
			FileInputStream fileIn = new FileInputStream("data/" + strTableName + "-" + s1 + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			if (!(type.equals("java.awt.Polygon")))
				tree = (BPTree) in.readObject();
			else
				tree1 = (RTree) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		String f = getType(strTableName);
		switch (f) {
		case ("java.lang.String"): {
			refer = tree.search(strClusteringKey);
			break;

		}
		case ("java.lang.Integer"): {
			refer = tree.search(Integer.parseInt(strClusteringKey));
			break;

		}
		case ("java.lang.Double"): {
			refer = tree.search(Double.parseDouble(strClusteringKey));
			break;

		}
		case ("java.awt.Polygon"): {
			StringTokenizer st = new StringTokenizer(strClusteringKey, ",");
			int n = st.countTokens();
			int z = 0;
			int yindex = 0;
			int[] x = new int[n / 2];
			int[] y = new int[n / 2];
			while (st.hasMoreTokens()) {
				String s = st.nextToken();
				if (s.charAt(0) == '(') {
					x[z] = Integer.parseInt(s.substring(1));
					z++;
				} else {
					y[yindex] = Integer.parseInt(s.substring(0, s.length() - 1));
					yindex++;
				}
			}
			po = new Polygon(x, y, n / 2);
			refer = tree1.search(new PolygonDB(po).getArea());

			break;
		}
		case ("java.util.Date"): {
			SimpleDateFormat f1 = new SimpleDateFormat("yyyy-MM-dd");
			Date d = f1.parse(strClusteringKey);
			refer = tree.search(d);
			break;

		}
		}
		try {
			FileOutputStream fileOut = new FileOutputStream("data/" + strTableName + "-" + s1 + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			if (!(type.equals("java.awt.Polygon")))
				out.writeObject(tree);
			else
				out.writeObject(tree1);
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved");
		} catch (IOException e) {
			e.printStackTrace();
		}

		int CurrentOpenPage = 0;
		for (int i = 0; i < refer.size(); i++) {
			Ref reference = new Ref(refer.get(i).getPage(), refer.get(i).getIndexInPage());

			if (CurrentOpenPage != refer.get(i).getPage()) {
				try {
					FileInputStream fileIn = new FileInputStream(
							"data/" + strTableName + refer.get(i).getPage() + ".class");
					ObjectInputStream in = new ObjectInputStream(fileIn);
					page = (Vector<Hashtable<String, Object>>) in.readObject();
					in.close();
					fileIn.close();
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
				CurrentOpenPage = refer.get(i).getPage();
			}

			Hashtable h = page.get(refer.get(i).getIndexInPage());

			if (type.equals("java.awt.Polygon")) {
				if (new PolygonDB((Polygon) h.get(s1)).equals(po)) {

					Set<String> keySet = htblColNameValue.keySet();
					for (String key : keySet) {
						h.replace(key, htblColNameValue.get(key));
						h.replace("TouchDate", Calendar.getInstance().getTime());
					}
					page.set(refer.get(i).getIndexInPage(), h);

					updateSec(strTableName, htblColNameValue, s1, reference);
				}
			} else {
				Set<String> keySet = htblColNameValue.keySet();
				for (String key : keySet) {
					h.replace(key, htblColNameValue.get(key));
					h.replace("TouchDate", Calendar.getInstance().getTime());
				}
				page.set(refer.get(i).getIndexInPage(), h);

				updateSec(strTableName, htblColNameValue, s1, reference);

			}
			if (i + 1 == refer.size() || CurrentOpenPage != refer.get(i + 1).getPage()) {
				try {
					FileOutputStream fileOut = new FileOutputStream(
							"data/" + strTableName + refer.get(i).getPage() + ".class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(page);
					out.close();
					fileOut.close();
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
		}
	}

	public void updateSec(String strTableName, Hashtable<String, Object> htblColNameValue, String s1, Ref reference)
			throws FileNotFoundException, ClassNotFoundException {

		// System.out.println(reference);
		Vector<String> trees = IndexedMeta(strTableName);
		BPTree tree = null;
		RTree tree1 = null;

		for (int j = 0; j < trees.size(); j++) {
			Vector<Ref> v3;
			if (!trees.get(j).equals(s1) && htblColNameValue.containsKey(trees.get(j))) {
				String type = getKeyType(strTableName, trees.get(j));
				// System.out.println(trees.get(j));
				try {
					FileInputStream fileIn = new FileInputStream(
							"data/" + strTableName + "-" + trees.get(j) + ".class");
					ObjectInputStream in = new ObjectInputStream(fileIn);
					if (!(type.equals("java.awt.Polygon")))
						tree = (BPTree) in.readObject();
					else
						tree1 = (RTree) in.readObject();
					in.close();
					fileIn.close();
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
				if (!(type.equals("java.awt.Polygon")))
					tree.deleteByRef(reference);
				else
					tree1.deleteByRef(reference);

				if (htblColNameValue.get(trees.get(j)) instanceof String) {
					v3 = tree.search((String) (htblColNameValue.get(trees.get(j))));
					if (v3 == null) {
						v3 = new Vector<Ref>();
						v3.add(reference);
						tree.insert((String) (htblColNameValue.get(trees.get(j))), v3);
					} else if (v3.size() >= 1) {
						int k;
						for (k = 0; k < v3.size(); k++) {
							if (v3.get(k).getPage() > reference.getPage()) {
								break;
							} else if (v3.get(k).getPage() == reference.getPage()
									&& v3.get(k).getIndexInPage() > reference.getIndexInPage()) {
								break;
							}
						}

						v3.add(k, reference);
						tree.update((String) (htblColNameValue.get(trees.get(j))), v3);
					}

				}

				else if (htblColNameValue.get(trees.get(j)) instanceof Integer) {
					v3 = tree.search((Integer) (htblColNameValue.get(trees.get(j))));
					if (v3 == null) {
						v3 = new Vector<Ref>();
						v3.add(reference);
						tree.insert((Integer) (htblColNameValue.get(trees.get(j))), v3);
					} else if (v3.size() >= 1) {
						int k;
						for (k = 0; k < v3.size(); k++) {
							if (v3.get(k).getPage() > reference.getPage()) {
								break;
							} else if (v3.get(k).getPage() == reference.getPage()
									&& v3.get(k).getIndexInPage() > reference.getIndexInPage()) {
								break;
							}
						}

						v3.add(k, reference);
						tree.update((Integer) (htblColNameValue.get(trees.get(j))), v3);
					}

				}

				else if (htblColNameValue.get(trees.get(j)) instanceof Double) {
					v3 = tree.search((Double) (htblColNameValue.get(trees.get(j))));
					if (v3 == null) {
						v3 = new Vector<Ref>();
						v3.add(reference);
						tree.insert((Double) (htblColNameValue.get(trees.get(j))), v3);
					} else if (v3.size() >= 1) {
						int k;
						for (k = 0; k < v3.size(); k++) {
							if (v3.get(k).getPage() > reference.getPage()) {
								break;
							} else if (v3.get(k).getPage() == reference.getPage()
									&& v3.get(k).getIndexInPage() > reference.getIndexInPage()) {
								break;
							}
						}

						v3.add(k, reference);
						tree.update((Double) (htblColNameValue.get(trees.get(j))), v3);
					}

				}

				else if (htblColNameValue.get(trees.get(j)) instanceof Date) {
					v3 = tree.search((Date) (htblColNameValue.get(trees.get(j))));
					if (v3 == null) {
						v3 = new Vector<Ref>();
						v3.add(reference);
						tree.insert((Date) (htblColNameValue.get(trees.get(j))), v3);
					} else if (v3.size() >= 1) {
						int k;
						for (k = 0; k < v3.size(); k++) {
							if (v3.get(k).getPage() > reference.getPage()) {
								break;
							} else if (v3.get(k).getPage() == reference.getPage()
									&& v3.get(k).getIndexInPage() > reference.getIndexInPage()) {
								break;
							}
						}

						v3.add(k, reference);
						tree.update((Date) (htblColNameValue.get(trees.get(j))), v3);
					}
				}

				else if (htblColNameValue.get(trees.get(j)) instanceof Polygon) {

					v3 = tree1.search(new PolygonDB((Polygon) (htblColNameValue.get(trees.get(j)))).getArea());
					if (v3 == null) {
						v3 = new Vector<Ref>();
						v3.add(reference);
						tree1.insert(new PolygonDB((Polygon) (htblColNameValue.get(trees.get(j)))).getArea(), v3);
					} else if (v3.size() >= 1) {
						int k;
						for (k = 0; k < v3.size(); k++) {
							if (v3.get(k).getPage() > reference.getPage()) {
								break;
							} else if (v3.get(k).getPage() == reference.getPage()
									&& v3.get(k).getIndexInPage() > reference.getIndexInPage()) {
								break;
							}
						}

						v3.add(k, reference);
						tree1.update(new PolygonDB((Polygon) (htblColNameValue.get(trees.get(j)))).getArea(), v3);
					}
				}

				try {
					FileOutputStream fileOut = new FileOutputStream(
							"data/" + strTableName + "-" + trees.get(j) + ".class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					if (!(type.equals("java.awt.Polygon")))
						out.writeObject(tree);
					else
						out.writeObject(tree1);
					out.close();
					fileOut.close();
					// System.out.println("Serialized data is saved in " + trees.get(j));
				} catch (IOException e) {
					e.printStackTrace();
				}

			}

		}
	}

	public Boolean matchesd(String strTableName, Hashtable<String, Object> htblColNameValue) throws IOException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		boolean f = true;
		// sc.useDelimiter("[,\n]");

		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		Object x = null;
		// int i = tablesize(strTableName);
		// if (htblColNameValue.size() - 1 == i) {
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				// System.out.println(scTableName);
				scColName = l[1];
				// System.out.println(scColName);
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				// System.out.println(scColName);
				if (scTableName.equals(strTableName) && htblColNameValue.containsKey(scColName)) {
					// System.out.println("1");
					switch (scColType) {
					case ("java.lang.String"):
						if (!(htblColNameValue.get(scColName) instanceof String)) {

							f = false;

						}
						break;
					case ("java.lang.Integer"):
						if (!(htblColNameValue.get(scColName) instanceof Integer)) {

							f = false;

						}
						break;
					case ("java.lang.Double"):
						if (!(htblColNameValue.get(scColName) instanceof Double)) {

							f = false;

						}
						break;
					case ("java.awt.Polygon"):
						if (!(htblColNameValue.get(scColName) instanceof Polygon)) {
							f = false;

						}
						break;
					case ("java.lang.Boolean"):
						if (!(htblColNameValue.get(scColName) instanceof Boolean)) {
							f = false;

						}
						break;
					case ("java.util.Date"):
						if (!(htblColNameValue.get(scColName) instanceof Date)) {
							f = false;

						}
						break;

					}

				}

			}
		}
		// } else {
		// System.out.println(htblColNameValue.size() + " " + i);
		// return false;
		// }
		return f;
	}

	public Vector<String> getIndexedDelete(String tableName, Hashtable<String, Object> h) throws FileNotFoundException {
		Vector<String> v = new Vector<String>();
		Set<String> keySet = h.keySet();
		for (String key : keySet) {
			if (checkIndexed(tableName, key)) {
				v.add(key);
			}
		}
		return v;
	}

	public Vector<String> getUnindexedDelete(String tableName, Hashtable<String, Object> h)
			throws FileNotFoundException {
		Vector<String> v = new Vector<String>();
		Set<String> keySet = h.keySet();
		for (String key : keySet) {
			if (!checkIndexed(tableName, key)) {
				v.add(key);
			}
		}
		return v;
	}

	public void deleteIndexed(String tableName, Hashtable<String, Object> h, Vector<String> indexed,
			Vector<String> unindexed) throws ClassNotFoundException, FileNotFoundException, DBAppException {
		int fl = lastFile(tableName);
		Vector<Vector<Ref>> refer = new Vector<Vector<Ref>>();
		Vector<Ref> referFinal = new Vector<Ref>();
		Vector<BPTree> treeVector = new Vector<BPTree>();
		Vector<RTree> treeVector1 = new Vector<RTree>();
		String keyName = "";
		BPTree tree = null;
		RTree tree1 = null;
		for (int i = 0; i < indexed.size(); i++) {
			Vector<Ref> v = new Vector<Ref>();
			keyName = indexed.get(i);
			String type = getKeyType(tableName, keyName);
			try {
				FileInputStream fileIn = new FileInputStream("data/" + tableName + "-" + keyName + ".class");
				ObjectInputStream in = new ObjectInputStream(fileIn);
				if (!(type.equals("java.awt.Polygon"))) {
					tree = (BPTree) in.readObject();
					treeVector.add(tree);
				} else {
					tree1 = (RTree) in.readObject();
					treeVector1.add(tree1);
				}
				in.close();
				fileIn.close();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}

			switch (type) {
			case ("java.lang.String"):
				v = tree.search((String) h.get(keyName));
				break;
			case ("java.lang.Integer"):
				v = tree.search((Integer) h.get(keyName));
				break;
			case ("java.lang.Double"):
				v = tree.search((Double) h.get(keyName));
				break;
			case ("java.awt.Polygon"): {
				Vector<Ref> v2 = new Vector<Ref>();
				Vector page = new Vector();
				Hashtable h1 = new Hashtable();
				v = tree1.search((Double) (new PolygonDB((Polygon) h.get(keyName))).getArea());

				int CurrentOpenPage = 0;
				if (v != null) {
					for (int i1 = 0; i1 < v.size(); i1++) {
						Ref reference = new Ref(v.get(i1).getPage(), v.get(i1).getIndexInPage());

						if (CurrentOpenPage != v.get(i1).getPage()) {
							try {
								FileInputStream fileIn = new FileInputStream(
										"data/" + tableName + v.get(i1).getPage() + ".class");
								ObjectInputStream in = new ObjectInputStream(fileIn);
								page = (Vector<Hashtable<String, Object>>) in.readObject();
								in.close();
								fileIn.close();
							} catch (IOException e) {
								e.printStackTrace();
								return;
							}
							CurrentOpenPage = v.get(i1).getPage();
						}

						h1 = (Hashtable) page.get(v.get(i1).getIndexInPage());
						if (new PolygonDB((Polygon) h1.get(keyName)).equals((Polygon) h.get(keyName))) {
							v2.add(v.get(i1));
						}

						if (i1 + 1 == v.size() || CurrentOpenPage != v.get(i1 + 1).getPage()) {
							try {
								FileOutputStream fileOut = new FileOutputStream(
										"data/" + tableName + CurrentOpenPage + ".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(page);
								out.close();
								fileOut.close();
								// System.out.println("Serialized data is saved");
							} catch (IOException e) {
								e.printStackTrace();
							}

						}

					}
				}
				v = v2;

				break;
			}
			case ("java.util.Date"):
				v = tree.search((Date) h.get(keyName));
				break;
			}
			if (v != null && v.size() != 0) {
				refer.add(v);
			}

			try {
				FileOutputStream fileOut = new FileOutputStream("data/" + tableName + "-" + keyName + ".class");
				ObjectOutputStream out = new ObjectOutputStream(fileOut);
				if (!(type.equals("java.awt.Polygon"))) {
					out.writeObject(tree);
				} else
					out.writeObject(tree1);
				out.close();
				fileOut.close();
				// System.out.println("Serialized data is saved");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// System.out.println(refer);
		if (refer.size() != 0 && refer.get(0) != null) {
			for (int i = 0; i < refer.get(0).size(); i++) {
				int count = 0;
				for (int j = 1; j < refer.size(); j++) {
					for (int z = 0; z < refer.get(j).size(); z++) {
						if (refer.get(j).get(z).getPage() == refer.get(0).get(i).getPage()
								&& refer.get(j).get(z).getIndexInPage() == refer.get(0).get(i).getIndexInPage()) {
							count++;
						}

					}
				}
				if (count == refer.size() - 1) {
					referFinal.add(refer.get(0).get(i));
				}

			}
			// System.out.println(referFinal);

			Vector<Hashtable<String, Object>> v2 = new Vector<Hashtable<String, Object>>();
			Vector<Vector> deletingPages = new Vector<Vector>();
			deletingPages.add(new Vector());

//// Populating deleting Pages

			int c = 1;
			boolean d = true;
			while (d) {
				File file = new File("data/" + tableName + c + ".class");
				if (!file.exists()) {
					d = false;
				} else {
					c++;
					deletingPages.add(new Vector());
				}

			}
			// System.out.println(deletingPages);

			for (int i = 0; i < referFinal.size(); i++) {
				Vector<Hashtable<String, Object>> vector = new Vector<Hashtable<String, Object>>();
				try {
					FileInputStream fileIn = new FileInputStream(
							"data/" + tableName + referFinal.get(i).getPage() + ".class");
					ObjectInputStream in = new ObjectInputStream(fileIn);

					vector = (Vector) in.readObject();
					in.close();
					fileIn.close();
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
				deletingPages.set(referFinal.get(i).getPage(), vector);
				try {
					FileOutputStream fileOut = new FileOutputStream(
							"data/" + tableName + referFinal.get(i).getPage() + ".class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					out.writeObject(vector);
					out.close();
					fileOut.close();
					// System.out.println("Serialized data is saved");
				} catch (IOException e) {
					e.printStackTrace();
				}

			}
			if (!unindexed.isEmpty()) {
				for (int i = 0; i < referFinal.size(); i++) {
					Vector<Hashtable<String, Object>> vs = deletingPages.get(referFinal.get(i).getPage());
					Hashtable<String, Object> hash = vs.get(referFinal.get(i).getIndexInPage());
					for (int j = 0; j < unindexed.size(); j++) {
						Object o = hash.get(unindexed.get(j));
						Object o2 = h.get(unindexed.get(j));

						if (o instanceof String) {
							if (!((String) o).equals((String) o2)) {
								referFinal.remove(i);
								i -= 1;
							}
						} else if (o instanceof Integer) {
							if (!((Integer) o).equals(((Integer) o2))) {

								referFinal.remove(i);
								i -= 1;
							}
						} else if (o instanceof Boolean) {
							if (!((Boolean) o).equals(((Boolean) o2))) {
								referFinal.remove(i);
								i -= 1;
							}
						} else if (o instanceof Double) {
							if (!((Double) o).equals(((Double) o2))) {
								referFinal.remove(i);
								i -= 1;
							}
						} else if (o instanceof Polygon) {
							if (!(new PolygonDB((Polygon) o)).equals((Polygon) o2)) {
								referFinal.remove(i);
								i -= 1;
							}
						} else if (o instanceof Date) {
							if (((Date) o).compareTo((Date) o2) != 0) {
								referFinal.remove(i);
								i -= 1;
							}
						}
					}
				}
			}
			Vector<String> totalTrees = getAllTrees(tableName);

			for (int j = 0; j < totalTrees.size(); j++) {
				String type = getKeyType(tableName, totalTrees.get(j));
				try {
					FileInputStream fileIn = new FileInputStream(
							"data/" + tableName + "-" + totalTrees.get(j) + ".class");
					ObjectInputStream in = new ObjectInputStream(fileIn);
					if (!(type.equals("java.awt.Polygon")))
						tree = (BPTree) in.readObject();
					else
						tree1 = (RTree) in.readObject();
					in.close();
					fileIn.close();
				} catch (IOException e) {
					e.printStackTrace();
					return;
				}
				for (int i = referFinal.size() - 1; i >= 0; i--) {
					// System.out.println("ASDFFFFFFFFFFFFSFFFSFFFSAFASDFdsgfDSG");
					if (!(type.equals("java.awt.Polygon"))) {
						tree.deleteByRef(referFinal.get(i));
						String s = totalTrees.get(j);
						if (s.equals(getKeyMeta(tableName))) {
							tree.update4(referFinal.get(i), n);
						} else {
							tree.update5(referFinal.get(i), n);
						}
					} else {
			//			System.out.println(referFinal.get(i));
						tree1.deleteByRef(referFinal.get(i));
						String s = totalTrees.get(j);
						if (s.equals(getKeyMeta(tableName))) {
							tree1.update4(referFinal.get(i), n);
						} else {
							tree1.update5(referFinal.get(i), n);
						}
					}

				}
				try {
					FileOutputStream fileOut = new FileOutputStream(
							"data/" + tableName + "-" + totalTrees.get(j) + ".class");
					ObjectOutputStream out = new ObjectOutputStream(fileOut);
					if (!(type.equals("java.awt.Polygon")))
						out.writeObject(tree);
					else
						out.writeObject(tree1);
					out.close();
					fileOut.close();
					// System.out.println("Serialized data is saved");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			Vector<Integer> notempty = new Vector<Integer>();
			for (int i = 0; i < deletingPages.size(); i++) {
				if (!deletingPages.get(i).isEmpty()) {
					notempty.add(i);
				}
			}
			for (int i = referFinal.size() - 1; i >= 0; i--) {
				(deletingPages.get(referFinal.get(i).getPage())).remove(referFinal.get(i).getIndexInPage());
			}
			for (int i = 0; i < deletingPages.size(); i++) {
				if (deletingPages.get(i) != null && deletingPages.get(i).size() != 0) {
					try {
						FileOutputStream fileOut = new FileOutputStream("data/" + tableName + i + ".class");
						ObjectOutputStream out = new ObjectOutputStream(fileOut);
						out.writeObject(deletingPages.get(i));
						out.close();
						fileOut.close();
						// System.out.println("Serialized data is saved");
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			for (int i = 0; i < deletingPages.size(); i++) {
				if (deletingPages.get(i) != null && deletingPages.get(i).size() == 0 && notempty.contains(i)) {
					File s = new File("data/" + tableName + i + ".class");
					s.delete();
					for (int x = i; x < fl; x++) {
						s = new File("data/" + tableName + (x + 1) + ".class");
						File f = new File("data/" + tableName + x + ".class");
						s.renameTo(f);
					}
					Vector<String> v = getAllTrees(tableName);
					for (int j = 0; j < v.size(); j++) {
						String type = getKeyType(tableName, v.get(j));
						try {
							FileInputStream fileIn = new FileInputStream(
									"data/" + tableName + "-" + v.get(j) + ".class");
							ObjectInputStream in = new ObjectInputStream(fileIn);
							if (!(type.equals("java.awt.Polygon")))
								tree = (BPTree) in.readObject();
							else
								tree1 = (RTree) in.readObject();
							in.close();
							fileIn.close();
						} catch (IOException e) {
							e.printStackTrace();
							return;
						}
						if (!(type.equals("java.awt.Polygon")))
							tree.deletedPage(i);
						else
							tree1.deletedPage(i);

						try {
							FileOutputStream fileOut = new FileOutputStream(
									"data/" + tableName + "-" + v.get(j) + ".class");
							ObjectOutputStream out = new ObjectOutputStream(fileOut);
							if (!(type.equals("java.awt.Polygon")))
								out.writeObject(tree);
							else
								out.writeObject(tree1);
							out.close();
							fileOut.close();
						} catch (IOException e) {
							e.printStackTrace();
						}

					}
				}
			}
		} else {
			throw new DBAppException("Key doesn't exist.");
		}

	}

	public Vector<String> getAllTrees(String tableName) throws FileNotFoundException {
		Vector<String> v = new Vector<String>();

		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		String f = "";
		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";

		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				scColName = l[1];
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				if (scTableName.equals(tableName) && scIndexed.toLowerCase().equals("true")) {
					v.add(scColName);
				}
			}
		}
		return v;
	}
	// Delete the record using the BTree
	// Delete el reference wel key fel BTree
	//

////////////////////////////	//DBApp( deleteFromTable,getIndexedDelete,getUnindexedDelete, deleteIndexed, getAllTrees)
	///////////////////////// /// BPTree(deleteByRef)
	///////////////////////// /// BPTreeNode(deleteByRef)
	////////////////////////// // BPTreeInnerNode(getKey2)
	///////////////////////// /// BPTreeLeafNode(deleteByRef)

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		Vector<Hashtable<String, Object>> vector;
		Hashtable h;
		boolean flag = false;
		int fl = lastFile(strTableName);
		try {
			if (matchesd(strTableName, htblColNameValue)) {
				Vector<String> indexedVector = getIndexedDelete(strTableName, htblColNameValue);
				Vector<String> unindexedVector = getUnindexedDelete(strTableName, htblColNameValue);
				if (indexedVector.size() == 0) {
					String tkey = getKeyMeta(strTableName);
					if (!htblColNameValue.containsKey(tkey)) {
						for (int i = 1; i <= lastFile(strTableName); i++) {

							try {
								FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
								ObjectInputStream in = new ObjectInputStream(fileIn);
								vector = (Vector) in.readObject();
								in.close();
								fileIn.close();
							} catch (IOException e) {
								e.printStackTrace();
								return;
							}
							for (int j = 0; j < vector.size(); j++) {
								h = vector.get(j);
								Boolean f = true;

								Set<String> keySet = htblColNameValue.keySet();
								for (String key : keySet) {
									String type = getKeyType(strTableName, key);
									switch (type) {
									case ("java.lang.String"):
										if (!(((String) h.get(key)).equals((String) htblColNameValue.get(key)))) {
											f = false;
										}
										break;
									case ("java.lang.Integer"):
										if (((Integer) h.get(key))
												.compareTo((Integer) htblColNameValue.get(key)) != 0) {
											f = false;
										}
										break;
									case ("java.lang.Double"):
										if (((Double) h.get(key)).compareTo((Double) htblColNameValue.get(key)) != 0) {
											f = false;
										}
										break;
									case ("java.awt.Polygon"):
										if (!(new PolygonDB((Polygon) h.get(key)))
												.equals((Polygon) htblColNameValue.get(key))) {
											f = false;
										}
										break;
									case ("java.lang.Boolean"):
										if (((Boolean) h.get(key))
												.compareTo((Boolean) htblColNameValue.get(key)) != 0) {
											f = false;
										}
										break;
									case ("java.util.Date"):
										if (((Date) h.get(key)).compareTo((Date) htblColNameValue.get(key)) != 0) {
											f = false;
										}
										break;

									}
									if (!f)
										break;

								}
								if (f) {
									vector.remove(j);
									Ref r = new Ref(i, j);
									Vector<String> v = getAllTrees(strTableName);
									BPTree tree = new BPTree(node);
									RTree tree1 = new RTree(node);

									for (int z = 0; z < v.size(); z++) {
										String type = getKeyType(strTableName, v.get(z));
										try {
											FileInputStream fileIn = new FileInputStream(
													"data/" + strTableName + "-" + v.get(z) + ".class");
											ObjectInputStream in = new ObjectInputStream(fileIn);
											if (!(type.equals("java.awt.Polygon")))
												tree = (BPTree) in.readObject();
											else
												tree1 = (RTree) in.readObject();
											in.close();
											fileIn.close();
										} catch (IOException e) {
											e.printStackTrace();
											return;
										}
										if (!(type.equals("java.awt.Polygon"))) {
											tree.deleteByRef(r);
											if (vector.size() == 0)
												tree.deletedPage(i);
											else {
												if (v.get(z).equals(getKeyMeta(strTableName))) {
													tree.update4(r, n);
												} else {
													tree.update5(r, n);
												}
											}
											try {
												FileOutputStream fileOut = new FileOutputStream(
														"data/" + strTableName + "-" + v.get(z) + ".class");
												ObjectOutputStream out = new ObjectOutputStream(fileOut);
												out.writeObject(tree);
												out.close();
												fileOut.close();
												// System.out.println("Serialized data is saved");
											} catch (IOException e) {
												e.printStackTrace();
											}

										} else {
											tree1.deleteByRef(r);
											if (vector.size() == 0)
												tree1.deletedPage(i);
											else {
												if (v.get(z).equals(getKeyMeta(strTableName))) {
													tree1.update4(r, n);
												} else {
													tree1.update5(r, n);
												}
											}

											try {
												FileOutputStream fileOut = new FileOutputStream(
														"data/" + strTableName + "-" + v.get(z) + ".class");
												ObjectOutputStream out = new ObjectOutputStream(fileOut);
												out.writeObject(tree1);
												out.close();
												fileOut.close();
												// System.out.println("Serialized data is saved");
											} catch (IOException e) {
												e.printStackTrace();
											}
										}

									}
									flag = true;
									j--;
								}

							}
							try {
								FileOutputStream fileOut = new FileOutputStream("data/" + strTableName + i + ".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(vector);
								out.close();
								fileOut.close();
								// System.out.println("Serialized data is saved");
							} catch (IOException e) {
								e.printStackTrace();
							}

							if (vector.size() == 0) {
								File s = new File("data/" + strTableName + i + ".class");
								s.delete();
								for (int x = i; x < fl; x++) {
									s = new File("data/" + strTableName + (x + 1) + ".class");
									File f = new File("data/" + strTableName + x + ".class");
									s.renameTo(f);
								}
								i--;
							}

						}
						if (!flag)
							throw new DBAppException("Record not found.");
					} else {
						Object o = htblColNameValue.get(tkey);
						for (int i = 1; i <= lastFile(strTableName); i++) {

							try {
								FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
								ObjectInputStream in = new ObjectInputStream(fileIn);
								vector = (Vector) in.readObject();
								in.close();
								fileIn.close();
							} catch (IOException e) {
								e.printStackTrace();
								return;
							}
							int[] index = binarySearchIndex(vector, o, tkey);
							for (int j = index[1]; index[0] >= 0 && j >= index[0]; j--) {
								Boolean f = true;
								Hashtable h1 = vector.get(j);
								Set<String> keySet = htblColNameValue.keySet();
								for (String key : keySet) {
									String type = getKeyType(strTableName, key);
									switch (type) {
									case ("java.lang.String"):
										if (!(((String) h1.get(key)).equals((String) htblColNameValue.get(key)))) {
											f = false;
										}
										break;
									case ("java.lang.Integer"):
										if (((Integer) h1.get(key))
												.compareTo((Integer) htblColNameValue.get(key)) != 0) {
											f = false;
										}
										break;
									case ("java.lang.Double"):
										if (((Double) h1.get(key)).compareTo((Double) htblColNameValue.get(key)) != 0) {
											f = false;
										}
										break;
									case ("java.awt.Polygon"):
										if (!(new PolygonDB((Polygon) h1.get(key)))
												.equals((Polygon) htblColNameValue.get(key))) {
											f = false;
										}
										break;
									case ("java.lang.Boolean"):
										if (((Boolean) h1.get(key))
												.compareTo((Boolean) htblColNameValue.get(key)) != 0) {
											f = false;
										}
										break;
									case ("java.util.Date"):
										if (((Date) h1.get(key)).compareTo((Date) htblColNameValue.get(key)) != 0) {
											f = false;
										}
										break;

									}
									if (!f)
										break;

								}
								if (f) {
									vector.remove(j);
									Ref r = new Ref(i, j);
									Vector<String> v = getAllTrees(strTableName);
									BPTree tree = new BPTree(node);
									RTree tree1 = new RTree(node);

									for (int z = 0; z < v.size(); z++) {
										String type = getKeyType(strTableName, v.get(z));
										try {
											FileInputStream fileIn = new FileInputStream(
													"data/" + strTableName + "-" + v.get(z) + ".class");
											ObjectInputStream in = new ObjectInputStream(fileIn);
											if (!(type.equals("java.awt.Polygon")))
												tree = (BPTree) in.readObject();
											else
												tree1 = (RTree) in.readObject();
											in.close();
											fileIn.close();
										} catch (IOException e) {
											e.printStackTrace();
											return;
										}
										if (!(type.equals("java.awt.Polygon"))) {
											tree.deleteByRef(r);
											if (vector.size() == 0)
												tree.deletedPage(i);
											else {
												if (v.get(z).equals(getKeyMeta(strTableName))) {
													tree.update4(r, n);
												} else {
													tree.update5(r, n);
												}
											}
											try {
												FileOutputStream fileOut = new FileOutputStream(
														"data/" + strTableName + "-" + v.get(z) + ".class");
												ObjectOutputStream out = new ObjectOutputStream(fileOut);
												out.writeObject(tree);
												out.close();
												fileOut.close();
												// System.out.println("Serialized data is saved");
											} catch (IOException e) {
												e.printStackTrace();
											}

										} else {
											tree1.deleteByRef(r);
											if (vector.size() == 0)
												tree1.deletedPage(i);
											else {
												if (v.get(z).equals(getKeyMeta(strTableName))) {
													tree1.update4(r, n);
												} else {
													tree1.update5(r, n);
												}
											}

											try {
												FileOutputStream fileOut = new FileOutputStream(
														"data/" + strTableName + "-" + v.get(z) + ".class");
												ObjectOutputStream out = new ObjectOutputStream(fileOut);
												out.writeObject(tree1);
												out.close();
												fileOut.close();
												// System.out.println("Serialized data is saved");
											} catch (IOException e) {
												e.printStackTrace();
											}
										}

									}
									flag = true;

								}

							}
							try {
								FileOutputStream fileOut = new FileOutputStream("data/" + strTableName + i + ".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(vector);
								out.close();
								fileOut.close();
								// System.out.println("Serialized data is saved");
							} catch (IOException e) {
								e.printStackTrace();
							}

							if (vector.size() == 0) {
								File s = new File("data/" + strTableName + i + ".class");
								s.delete();
								for (int x = i; x < fl; x++) {
									s = new File("data/" + strTableName + (x + 1) + ".class");
									File f = new File("data/" + strTableName + x + ".class");
									s.renameTo(f);
								}
								i--;
							}
							if (index[1] < this.n - 1 && index[0] != -1)
								break;
						}
						if (!flag)
							throw new DBAppException("Record not found.");

					}

				} else {

					deleteIndexed(strTableName, htblColNameValue, indexedVector, unindexedVector);
				}
				if (!checkFile(strTableName)) {
					Vector v = getAllTrees(strTableName);
					for (int i = 0; i < v.size(); i++) {
						File f = new File("data/" + strTableName + "-" + v.get(i) + ".class");
						f.delete();
					}
				}
////////////////////////////////////////// DO NOT FORGET R TREE!!!!!
			} else {
				throw new DBAppException("Record invalid");
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public String getKeyType(String strTableName, String key) throws FileNotFoundException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		String f = "";
		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				// System.out.println(scTableName);
				scColName = l[1];
				// System.out.println(scColName);
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				// System.out.println(scColName);
				if (scTableName.equals(strTableName) && scColName.equals(key)) {
					f = scColType;
				}
			}
		}
		return f;

	}

	// M2--------------------------------------------------------

	public void createBTreeIndex(String strTableName, String strColName) throws DBAppException {

		try {
			if (checkE(strTableName, strColName)) {
				String type = getKeyType(strTableName, strColName);
				if (!type.equals("java.awt.Polygon")) {
					File file1 = new File("data/metadata.csv");
					File file2 = new File("data/metadata.csv");
//			file1.renameTo(new File("data/temp.csv"));
					try {
						Scanner sc = new Scanner(file1);
						// sc.useDelimiter("[,\n]");

						String scTableName, scColName, scColType, scKey, scIndexed;
						String line = "";
						int i = 0;
						while (sc.hasNextLine()) {
							// System.out.println("ay 7aga");
							line = sc.nextLine();
							String[] l = line.split(",");
							if (l.length == 5) {
								scTableName = l[0];
								scColName = l[1];
								scColType = l[2];
								scKey = l[3];
								scIndexed = l[4];
								if (scTableName.equals(strTableName) && scColName.equals(strColName)) {
									// System.out.println("exists");
									l[4] = "true";
									line = l[0] + "," + l[1] + "," + l[2] + "," + l[3] + "," + l[4];

								}
								if (i == 0) {
									try {
										FileWriter fw = new FileWriter(file2, false);
										BufferedWriter bw = new BufferedWriter(fw);
										// PrintWriter pw = new PrintWriter(bw);
										bw.write(line + "\n");
										bw.close();
										fw.close();

									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								} else {
									try {
										FileWriter fw = new FileWriter(file2, true);
										BufferedWriter bw = new BufferedWriter(fw);
										// PrintWriter pw = new PrintWriter(bw);
										bw.write(line + "\n");
										bw.close();
										fw.close();

									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}

							}
							i++;

						}
//                 if(file1.exists()&& file1.isFile()) {			
//				System.out.println(file1.delete());
//				}
//                 System.out.println("lol");
						file2.createNewFile();
//			    file2.renameTo(file1);
					} catch (Exception e) {
						System.out.println(e.getMessage());
					}

					BPTree tree;
					Vector<Hashtable<String, Object>> vector;
					if (checkFile(strTableName)) {
						switch (type) {
						case ("java.lang.String"):
							tree = new BPTree<String>(this.node);
							for (int i = 1; i <= lastFile(strTableName); i++) {

								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////					
								for (int j = 0; j < vector.size(); j++) {
									Vector v = tree.search((String) (vector.get(j).get(strColName)));
									if (v == null) {
										v = new Vector<Ref>();
										v.add(new Ref(i, j));
										tree.insert((String) (vector.get(j).get(strColName)), v);
									} else if (v.size() >= 1) {
										v.add(new Ref(i, j));
										tree.update((String) (vector.get(j).get(strColName)), v);
									}
								}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							}
							try {
								FileOutputStream fileOut = new FileOutputStream(
										"data/" + strTableName + "-" + strColName + ".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(tree);
								out.close();
								fileOut.close();
								// System.out.println("first");
							} catch (IOException e) {
								e.printStackTrace();
							}
							break;
						case ("java.lang.Integer"):
							tree = new BPTree<Integer>(this.node);
							for (int i = 1; i <= lastFile(strTableName); i++) {

								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
/////////////////////////////////////////////////////////////////////////////////////////////					
								for (int j = 0; j < vector.size(); j++) {
									Vector v = tree.search((Integer) (vector.get(j).get(strColName)));
									if (v == null) {
										v = new Vector<Ref>();
										v.add(new Ref(i, j));
										tree.insert((Integer) (vector.get(j).get(strColName)), v);
									} else if (v.size() >= 1) {
										v.add(new Ref(i, j));
										tree.update((Integer) (vector.get(j).get(strColName)), v);
									}
								}
//////////////////////////////////////////////////////////////////////////////////////////////////
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							}
							try {
								FileOutputStream fileOut = new FileOutputStream(
										"data/" + strTableName + "-" + strColName + ".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(tree);
								out.close();
								fileOut.close();
								// System.out.println("first");
							} catch (IOException e) {
								e.printStackTrace();
							}
							break;
						case ("java.lang.Double"):
							tree = new BPTree<Double>(this.node);
							for (int i = 1; i <= lastFile(strTableName); i++) {

								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
/////////////////////////////////////////////////////////////////////////////////////				
								for (int j = 0; j < vector.size(); j++) {
									Vector v = tree.search((Double) (vector.get(j).get(strColName)));
									if (v == null) {
										v = new Vector<Ref>();
										v.add(new Ref(i, j));
										tree.insert((Double) (vector.get(j).get(strColName)), v);
									} else if (v.size() >= 1) {
										v.add(new Ref(i, j));
										tree.update((Double) (vector.get(j).get(strColName)), v);
									}
								}
///////////////////////////////////////////////////////////////////////////////
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							}
							try {
								FileOutputStream fileOut = new FileOutputStream(
										"data/" + strTableName + "-" + strColName + ".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(tree);
								out.close();
								fileOut.close();
								// System.out.println("first");
							} catch (IOException e) {
								e.printStackTrace();
							}
							break;

						case ("java.util.Date"):
							tree = new BPTree<Date>(this.node);
							for (int i = 1; i <= lastFile(strTableName); i++) {

								try {
									FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
									ObjectInputStream in = new ObjectInputStream(fileIn);
									vector = (Vector) in.readObject();
									in.close();
									fileIn.close();
								} catch (IOException e) {
									e.printStackTrace();
									return;
								}
/////////////////////////////////////////////////////////////////////////////////////////////// :)))
								for (int j = 0; j < vector.size(); j++) {
									Vector v = tree.search((Date) (vector.get(j).get(strColName)));
									if (v == null) {
										v = new Vector<Ref>();
										v.add(new Ref(i, j));
										tree.insert((Date) (vector.get(j).get(strColName)), v);
									} else if (v.size() >= 1) {
										v.add(new Ref(i, j));
										tree.update((Date) (vector.get(j).get(strColName)), v);
									}
								}
////////////////////////////////////////////////////////////////////////////////////////////////
								try {
									FileOutputStream fileOut = new FileOutputStream(
											"data/" + strTableName + i + ".class");
									ObjectOutputStream out = new ObjectOutputStream(fileOut);
									out.writeObject(vector);
									out.close();
									fileOut.close();
									// System.out.println("first");
								} catch (IOException e) {
									e.printStackTrace();
								}

							}
							try {
								FileOutputStream fileOut = new FileOutputStream(
										"data/" + strTableName + "-" + strColName + ".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(tree);
								out.close();
								fileOut.close();
								// System.out.println("first");
							} catch (IOException e) {
								e.printStackTrace();
							}
							break;

						}

					}
				} else {
					throw new DBAppException("Cannot create BPtree on column of type polygon");
				}
			} else {
				throw new DBAppException("Table Name or Col Name is incorrect!!");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean checkE(String strTableName, String strColName) throws FileNotFoundException {

		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		boolean f = true;
		// sc.useDelimiter("[,\n]");

		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		Object x = null;
		// int i = tablesize(strTableName);
		// if (htblColNameValue.size() - 1 == i) {
		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				// System.out.println(scTableName);
				scColName = l[1];
				// System.out.println(scColName);
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				// System.out.println(scColName);
				if (scTableName.equals(strTableName) && strColName.equals(scColName)
						&& !scIndexed.toLowerCase().equals("true")) {
					return true;
				}
			}
		}
		return false;
	}

	public void createRTreeIndex(String strTableName, String strColName) throws DBAppException {
		try {
			if (checkE(strTableName, strColName)) {
				String type = getKeyType(strTableName, strColName);
				if (type.equals("java.awt.Polygon")) {

					File file1 = new File("data/metadata.csv");
					File file2 = new File("data/metadata.csv");
					try {
						Scanner sc = new Scanner(file1);
						String scTableName, scColName, scColType, scKey, scIndexed;
						String line = "";
						int i = 0;
						while (sc.hasNextLine()) {
							line = sc.nextLine();
							String[] l = line.split(",");
							if (l.length == 5) {
								scTableName = l[0];
								scColName = l[1];
								scColType = l[2];
								scKey = l[3];
								scIndexed = l[4];
								if (scTableName.equals(strTableName) && scColName.equals(strColName)) {
									l[4] = "true";
									line = l[0] + "," + l[1] + "," + l[2] + "," + l[3] + "," + l[4];

								}
								if (i == 0) {
									try {
										FileWriter fw = new FileWriter(file2, false);
										BufferedWriter bw = new BufferedWriter(fw);
										// PrintWriter pw = new PrintWriter(bw);
										bw.write(line + "\n");
										bw.close();
										fw.close();

									} catch (IOException e) {
										throw new DBAppException("File not Found");
									}
								} else {
									try {
										FileWriter fw = new FileWriter(file2, true);
										BufferedWriter bw = new BufferedWriter(fw);
										// PrintWriter pw = new PrintWriter(bw);
										bw.write(line + "\n");
										bw.close();
										fw.close();

									} catch (IOException e) {
										throw new DBAppException("File not Found");
									}
								}

							}
							i++;

						}

						file2.createNewFile();
					} catch (Exception e) {
						throw new DBAppException("File not Found");
					}
					RTree tree;
					Vector<Hashtable<String, Object>> vector;
					if (checkFile(strTableName)) {
						tree = new RTree(this.node);
						for (int i = 1; i <= lastFile(strTableName); i++) {

							try {
								FileInputStream fileIn = new FileInputStream("data/" + strTableName + i + ".class");
								ObjectInputStream in = new ObjectInputStream(fileIn);
								try {
									vector = (Vector) in.readObject();
								} catch (ClassNotFoundException e) {
									throw new DBAppException("Class not Found");
								}
								in.close();
								fileIn.close();
							} catch (IOException e) {
								e.printStackTrace();
								return;
							}
							///////////////////////////////////////////////////////////////////////////////////////////
							for (int j = 0; j < vector.size(); j++) {
								Vector v = tree.search(
										(Double) (new PolygonDB((Polygon) (vector.get(j).get(strColName))).getArea()));
								if (v == null) {
									v = new Vector<Ref>();
									v.add(new Ref(i, j));
									tree.insert((Double) (new PolygonDB((Polygon) (vector.get(j).get(strColName)))
											.getArea()), v);
								} else if (v.size() >= 1) {
									v.add(new Ref(i, j));
									tree.update((Double) (new PolygonDB((Polygon) (vector.get(j).get(strColName)))
											.getArea()), v);
								}
							}
							//////////////////////////////////////////////////////////////////////////////////////////
							try {
								FileOutputStream fileOut = new FileOutputStream("data/" + strTableName + i + ".class");
								ObjectOutputStream out = new ObjectOutputStream(fileOut);
								out.writeObject(vector);
								out.close();
								fileOut.close();
								// System.out.println("first");
							} catch (IOException e) {
								e.printStackTrace();
							}

						}
						try {
							FileOutputStream fileOut = new FileOutputStream(
									"data/" + strTableName + "-" + strColName + ".class");
							ObjectOutputStream out = new ObjectOutputStream(fileOut);
							out.writeObject(tree);
							out.close();
							fileOut.close();
							// System.out.println("first");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				} else {
					throw new DBAppException("Rtree cannot be created on this column type");
				}

			} else {
				throw new DBAppException("Table Name or Col Name is incorrect!!");
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			throw new DBAppException("File not Found");
		}

	}

	// everything under here: Yehia
	// selectFromable
	////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strArrOperators) throws DBAppException {
		Iterator iteratorFinal;
		// Checking phase:
		// checks if size of arrSQLTerms is not 0.
		// Checks if Operators in strArrOperators are apporpriate, if not throw error.//
		// checks if all the sql terms have the same table name. If not, throws error//
		// checks if the table name exists. if not throws error. checkMeta()
		// checks if the operators inside SQLTerm are appropriate. If not, error.//
		// Checks if all the columns are part of the table.
		// Checks if the Objects in the sql terms match the type of the column they
		// belong to.
		if (arrSQLTerms.length != 0) {
			if (arrOperatorsCheck(strArrOperators)) {
				if (sameTableCheck(arrSQLTerms)) {
					try {
						if (checkMeta(arrSQLTerms[0]._strTableName)) {
							if (sqlOperatorCheck(arrSQLTerms)) {
								if (columnChecker(arrSQLTerms)) {
									//////////////////////////////////////////////////////////////////////////////////////////
									if (arrSQLTerms.length != strArrOperators.length + 1)
										throw new DBAppException("The number of operators is incorrect.");
									Vector<Vector<Ref>> referencesVector = new Vector<Vector<Ref>>();
									for (int i = 0; i < arrSQLTerms.length; i++) {
										referencesVector.add(new Vector<Ref>());
									}

									// if there are operators:
									if (strArrOperators.length != 0) {

										int[] andIndex = andIndex(strArrOperators);
										// get reference vector of each indexed columns and put it in referencesVector
										Vector SQLTrees = getSQLTrees(arrSQLTerms);
										int treeCount = 0;
										for (int i = 0; i < arrSQLTerms.length; i++) {
											// may cause null pointer exception with treeCounter
											if (checkIndexed(arrSQLTerms[i]._strTableName,
													arrSQLTerms[i]._strColumnName)) {
												Vector<Ref> r = getIndexedQuery(arrSQLTerms[i],
														SQLTrees.get(treeCount));
												if (r != null)
													referencesVector.set(i, r);
												treeCount++;
											//	System.out.println("gpa " + referencesVector);
											} else {
												referencesVector.set(i,
														getUnindexedQueryRef(arrSQLTerms[i],
																checkClustering(arrSQLTerms[i]._strTableName,
																		arrSQLTerms[i]._strColumnName)));
											//	System.out.println("name " + referencesVector);

											}
										}
							//			System.out.println(referencesVector);
										// putting arrSQLTerms and strArrOperators in vectors to make it easier to deal
										// with
										Vector<SQLTerm> SQLTerms = new Vector<SQLTerm>();
										Vector<String> operators = new Vector<String>();
										for (SQLTerm s : arrSQLTerms)
											SQLTerms.add(s);
										for (String s : strArrOperators)
											operators.add(s);

										Vector<Ref> result = new Vector<Ref>();
										// Vector<Vector<Ref>> results = new Vector<Vector<Ref>>();
										// handling AND operations first
										for (int i : andIndex) {
											// System.out.println("aywan");
											// puts result of AND in result vector and all results in results vector
											result = handleAnd(SQLTerms.get(i), SQLTerms.get(i + 1),
													referencesVector.get(i), referencesVector.get(i + 1));
											// results.add(result);
											referencesVector.remove(i + 1);
											referencesVector.set(i, result);
											SQLTerms.remove(i);
											operators.remove(i);
											for (int j = 0; j < andIndex.length; j++)
												if (andIndex[j] > i)
													andIndex[j]--;
										}
										// System.out.println(results);
										// reducing the size of the vectors
										// since the handled sql terms will always have index references in
										// referenceVector,
										// we dont really care what the term that is being deleted is from the pair that
										// has
										// been given to handleAnd
					//					System.out.println(referencesVector);
//											for (int i = andIndex.length-1; i >=0; i--) {
//											System.out.println(andIndex[i] + 1);
//												referencesVector.remove(andIndex[i] + 1);
//												referencesVector.set(andIndex[i], results.get(i));
//												SQLTerms.remove(andIndex[i]);
//												operators.remove(andIndex[i]);
//											}
//											// handling the OR's and XOR's
										while (operators.size() > 0) {
											Vector<Ref> temp = new Vector<Ref>();
											if (operators.get(0).equals("OR"))
												temp = handleOr(SQLTerms.get(0), SQLTerms.get(1),
														referencesVector.get(0), referencesVector.get(1));

											// could just be else but ana mbarwen nawww
											else if (operators.get(0).equals("XOR"))
												temp = handleXor(SQLTerms.get(0), SQLTerms.get(1),
														referencesVector.get(0), referencesVector.get(1));
											else if (operators.get(0).equals("AND"))
												throw new DBAppException("AND's have not all been deleted.");
											referencesVector.remove(0);
											referencesVector.set(0, temp);
											SQLTerms.remove(0);
											operators.remove(0);
										}
										// get all the records of the references in the remaining reference vector
										// in referencesVector.get(0)
										int maxPage = 0;
										Vector<Ref> finalRefs = referencesVector.get(0);
										Vector<Ref> r = (Vector<Ref>) finalRefs.clone();
										for (int i = finalRefs.size() - 1; i >= 0; i--) {
											r.remove(i);
											if (r.contains(finalRefs.get(i))) {
												finalRefs.remove(i);
											}
										}
										Vector<Ref> r1 = new Vector<Ref>();
										for (int i = finalRefs.size() - 1; i >= 0; i--) {
											boolean flag = true;
											for (int j = i - 1; j >= 0; j--) {
												if (finalRefs.get(i).getPage() == finalRefs.get(j).getPage()
														&& finalRefs.get(i).getIndexInPage() == finalRefs.get(j)
																.getIndexInPage()) {
													flag = false;
												}
											}
											if (flag) {
												r1.add(0, finalRefs.get(i));
											}
										}
										finalRefs = r1;
								//		System.out.println(finalRefs);
										for (int i = 0; i < finalRefs.size(); i++)
											if (finalRefs.get(i).getPage() > maxPage)
												maxPage = finalRefs.get(i).getPage();
										Vector<Vector<Hashtable<String, Object>>> pages = new Vector<Vector<Hashtable<String, Object>>>();
										pages.add(new Vector<Hashtable<String, Object>>());
										for (int i = 0; i < maxPage; i++) {
											pages.add(new Vector<Hashtable<String, Object>>());
										}
								//		System.out.println(pages);
										// puts pages used in finalRefs in their places after deserializing them
										for (int i = 0; i < finalRefs.size(); i++) {
											if (pages.get(finalRefs.get(i).getPage()).size() == 0) {
												pages.set(finalRefs.get(i).getPage(), deserializePage(
														SQLTerms.get(0)._strTableName, finalRefs.get(i).getPage()));
											}
										}
							//			System.out.println(pages);
										Vector<Hashtable<String, Object>> finalRecords = new Vector<Hashtable<String, Object>>();
										//
//										Vector<String> polygons = new Vector<String>();
//										Vector<Polygon> pos = new Vector<Polygon>();
//										for (int i = 0; i < arrSQLTerms.length; i++) {
//											if (arrSQLTerms[i]._objValue instanceof Polygon) {
//												polygons.add(arrSQLTerms[i]._strColumnName);
//												pos.add((Polygon) arrSQLTerms[i]._objValue);
//											}
//										}
										for (int i = 0; i < finalRefs.size(); i++) {
											Hashtable h = pages.get(finalRefs.get(i).getPage())
													.get(finalRefs.get(i).getIndexInPage());
//											boolean f = true;
//											for (int j = 0; j < polygons.size(); j++) {
//												PolygonDB p = new PolygonDB((Polygon) h.get(polygons.get(j)));
//												if (p.compareTo(pos.get(j)) == 0) {
//													if (!p.equals(pos.get(j))) {
//														f = false;
//													}
//
//												}
//											}
//											if (f)
											finalRecords.add(h);

										}
										iteratorFinal = finalRecords.iterator();
										return iteratorFinal;
									}

									// if there are no operators:
									else {
										// if the SQL Term we have is unindexed
										if (!checkIndexed(arrSQLTerms[0]._strTableName,
												arrSQLTerms[0]._strColumnName)) {
											Vector<Hashtable<String, Object>> result = (Vector<Hashtable<String, Object>>) getUnindexedQuery(
													arrSQLTerms[0], checkClustering(arrSQLTerms[0]._strTableName,
															arrSQLTerms[0]._strColumnName));
											iteratorFinal = result.iterator();
											return iteratorFinal;
										}
										// if the SQL Term we have is indexed
										else {
											Vector<Ref> references = new Vector<Ref>();

											Vector<Ref> ref = getIndexedQuery(arrSQLTerms[0],
													getSQLTrees(arrSQLTerms).get(0));
											if (ref != null)
												references = ref;
											Vector<Ref> r = (Vector<Ref>) references.clone();
											for (int i = references.size() - 1; i >= 0; i--) {
												r.remove(i);
												if (r.contains(references.get(i))) {
													references.remove(i);
												}
											}
											Vector<Integer> pageNums = new Vector<Integer>();
											for (int i = 0; i < references.size(); i++) {
												Integer pageNum = new Integer(references.get(i).getPage());
												if (!pageNums.contains(pageNum))
													pageNums.add(pageNum);
											}
											// getting maximum pageNum
											int max = 0;
											for (int i = 0; i < pageNums.size(); i++) {
												if (pageNums.get(i).intValue() > max)
													max = pageNums.get(i).intValue();
											}
											Vector<Vector<Hashtable<String, Object>>> pages = new Vector<Vector<Hashtable<String, Object>>>();
											pages.add(new Vector<Hashtable<String, Object>>());
											// making ordered vector of pages
											for (int i = 0; i <= max; i++) {
												pages.add(new Vector<Hashtable<String, Object>>());
											}
											for (int i = 0; i < pageNums.size(); i++) {
												pages.set(pageNums.get(i).intValue(), deserializePage(
														arrSQLTerms[0]._strTableName, pageNums.get(i).intValue()));
											}
											// deleting from pages
											Vector<Hashtable<String, Object>> results = new Vector<Hashtable<String, Object>>();
											for (int i = 0; i < references.size(); i++) {
												results.add(pages.get(references.get(i).getPage())
														.get(references.get(i).getIndexInPage()));
											}
											iteratorFinal = results.iterator();
											return iteratorFinal;

										}
									}

								} else
									throw new DBAppException(
											"One or more of the columns entered are incorrect. Or the types are incorrect");

							} else
								throw new DBAppException("An operator inside the sql terms is invalid.");

						} else
							throw new DBAppException("Table name doesnt exist.");

					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else
					throw new DBAppException("Must use only one table name");

			} else
				throw new DBAppException("Operators in strArrOperators are invalid.");

		} else
			throw new DBAppException("No Sql Terms have been entered");

		// Should not be reached
		iteratorFinal = (new Vector<Hashtable<String, Object>>()).iterator();
		return iteratorFinal;
	}

	// Checks if Operators in strArrOperators are apporpriate.
	public boolean arrOperatorsCheck(String[] strArrOperators) {
		if (strArrOperators.length == 0) {
			return true;
		} else {
			for (int i = 0; i < strArrOperators.length; i++)
				if (!(strArrOperators[i].equals("AND") || strArrOperators[i].equals("OR")
						|| strArrOperators[i].equals("XOR")))
					return false;
			return true;
		}
	}

	// checks if all the sql terms have the same table name. If not, throws error
	public boolean sameTableCheck(SQLTerm[] arrSQLTerms) {
		for (int i = 1; i < arrSQLTerms.length; i++)
			if (!arrSQLTerms[i]._strTableName.equals(arrSQLTerms[0]._strTableName))
				return false;
		return true;

	}

	// checks if the operators inside SQLTerm are appropriate
	public boolean sqlOperatorCheck(SQLTerm[] arrSQLTerms) {
		for (int i = 0; i < arrSQLTerms.length; i++)
			if (!(arrSQLTerms[i]._strOperator.equals("<") || arrSQLTerms[i]._strOperator.equals("<=")
					|| arrSQLTerms[i]._strOperator.equals(">") || arrSQLTerms[i]._strOperator.equals(">=")
					|| arrSQLTerms[i]._strOperator.equals("=") || arrSQLTerms[i]._strOperator.equals("!=")))
				return false;
		return true;
	}

	public void serializePage(Vector page, String tableName, int pageNum) {
		try {
			FileOutputStream fileOut = new FileOutputStream("data/" + tableName + pageNum + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(page);
			out.close();
			fileOut.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void serializeTree(BPTree tree, String tableName, String columnName) {
		try {
			FileOutputStream fileOut = new FileOutputStream("data/" + tableName + "-" + columnName + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(tree);
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void serializeTree(RTree tree, String tableName, String columnName) {
		try {
			FileOutputStream fileOut = new FileOutputStream("data/" + tableName + "-" + columnName + ".class");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(tree);
			out.close();
			fileOut.close();
			// System.out.println("Serialized data is saved");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Vector<Hashtable<String, Object>> deserializePage(String tableName, int pageNum) {
		Vector<Hashtable<String, Object>> vector;
		try {
			FileInputStream fileIn = new FileInputStream("data/" + tableName + pageNum + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			vector = (Vector<Hashtable<String, Object>>) in.readObject();
			in.close();
			fileIn.close();
			return vector;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}

	public BPTree deserializeBP(String tableName, String colName) {
		BPTree tree;
		try {
			FileInputStream fileIn = new FileInputStream("data/" + tableName + "-" + colName + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			tree = (BPTree) in.readObject();
			in.close();
			fileIn.close();
			return tree;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

	}

	public RTree deserializeR(String tableName, String colName) {
		RTree tree;
		try {
			FileInputStream fileIn = new FileInputStream("data/" + tableName + "-" + colName + ".class");
			ObjectInputStream in = new ObjectInputStream(fileIn);
			tree = (RTree) in.readObject();
			in.close();
			fileIn.close();
			return tree;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	// Checks if all columns in SQL array belong to the table
	// makes each column in sqlColumns array null when it is found in the metadata
	// with the same table name
	// then returns true if all the values in the array are null and false if not
	// Also checks if the types match the metadata using the switch case
	public boolean columnChecker(SQLTerm[] arrSQLTerms) throws FileNotFoundException {
		File file1 = new File("data/metadata.csv");
		Scanner sc = new Scanner(file1);
		String tableName = arrSQLTerms[0]._strTableName;
		String scTableName, scColName, scColType, scKey, scIndexed;
		String line = "";
		String[] sqlColumns = new String[arrSQLTerms.length];
		for (int i = 0; i < arrSQLTerms.length; i++) {
			sqlColumns[i] = arrSQLTerms[i]._strColumnName;
		}

		while (sc.hasNextLine()) {
			line = sc.nextLine();
			String[] l = line.split(",");
			if (l.length == 5) {
				scTableName = l[0];
				// System.out.println(scTableName);
				scColName = l[1];
				// System.out.println(scColName);
				scColType = l[2];
				scKey = l[3];
				scIndexed = l[4];
				if (tableName.equals(scTableName)) {
					for (int i = 0; i < sqlColumns.length; i++) {
						if (sqlColumns[i] != null && sqlColumns[i].equals(scColName))
							switch (scColType) {
							case ("java.lang.Integer"): {
								if (arrSQLTerms[i]._objValue instanceof Integer)
									sqlColumns[i] = null;
								break;
							}
							case ("java.lang.String"): {
								if (arrSQLTerms[i]._objValue instanceof String)
									sqlColumns[i] = null;
								break;
							}
							case ("java.lang.Double"): {
								if (arrSQLTerms[i]._objValue instanceof Double)
									sqlColumns[i] = null;
								break;
							}
							case (" java.util.Date"): {
								if (arrSQLTerms[i]._objValue instanceof Date)
									sqlColumns[i] = null;
								break;
							}
							////////////////////////////////////////////////////////// law ha select polygon
							////////////////////////////////////////////////////////// yenfa3 a select b eh
							case ("java.awt.Polygon"): {
								if (arrSQLTerms[i]._objValue instanceof Polygon)
									sqlColumns[i] = null;
								break;
							}
							///////////////////////////////////////////////////////////////////////////////////////////////////////////////
							case ("java.lang.Boolean"): {
								if (arrSQLTerms[i]._objValue instanceof Boolean)
									sqlColumns[i] = null;
								break;
							}
							}

					}
				}
			}

		}
		for (int i = 0; i < sqlColumns.length; i++) {
			if (sqlColumns[i] != null)
				return false;
		}
		return true;
	}

	// Checks if given column is clustering key
	// may cause problems with the return.
	public boolean checkClustering(String tableName, String columnName) throws DBAppException {
		File file1 = new File("data/metadata.csv");
		try {
			Scanner sc = new Scanner(file1);
			boolean flag = false;
			String scTableName, scColName, scColType, scKey, scIndexed;
			String line = "";
			while (sc.hasNextLine()) {
				line = sc.nextLine();
				String[] l = line.split(",");
				if (l.length == 5) {
					scTableName = l[0];
					// System.out.println(scTableName);
					scColName = l[1];
					// System.out.println(scColName);
					scColType = l[2];
					scKey = l[3];
					scIndexed = l[4];
					if (scTableName.equals(tableName) && scColName.equals(columnName))
						if (scKey.toLowerCase().equals("true"))
							return true;
						else
							return false;
				}

			}
			return false;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block

			e.printStackTrace();
			return false;
		}

	}

	// gets all indices of columns in query array
	public Vector getSQLTrees(SQLTerm[] arrSQLTerms) {
		Vector trees = new Vector();
		int j = 0;
		for (int i = 0; i < arrSQLTerms.length; i++) {
			try {
				if (checkIndexed(arrSQLTerms[i]._strTableName, arrSQLTerms[i]._strColumnName)) {
					String type = getKeyType(arrSQLTerms[i]._strTableName, arrSQLTerms[i]._strColumnName);
					if (!type.equals("java.awt.Polygon")) {
						trees.add(deserializeBP(arrSQLTerms[i]._strTableName, arrSQLTerms[i]._strColumnName));
						serializeTree((BPTree) trees.get(j), arrSQLTerms[i]._strTableName,
								arrSQLTerms[i]._strColumnName);
					} else {
						trees.add(deserializeR(arrSQLTerms[i]._strTableName, arrSQLTerms[i]._strColumnName));
						serializeTree((RTree) trees.get(j), arrSQLTerms[i]._strTableName,
								arrSQLTerms[i]._strColumnName);
					}
					j++;
				}

			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return trees;
	}

	// gets indices of the AND's in the arrOperands
	public int[] andIndex(String[] arrOperands) {
		int size = 0;
		for (int i = 0; i < arrOperands.length; i++) {
			if (arrOperands[i].equals("AND"))
				size++;
		}
		int[] indices = new int[size];
		if (size == 0)
			return indices;
		int index = 0;
		for (int i = 0; i < arrOperands.length; i++) {
			if (arrOperands[i].equals("AND")) {
				indices[index] = i;
				index++;
			}
		}
		return indices;
	}

	// method to get the result of a single sql term that is not an index when no
	// index is available
	// Should use binary search
	// first checks if the attribute given is a clustering key or not
	public Vector<Hashtable<String, Object>> getUnindexedQuery(SQLTerm query, boolean key) {
		String operator = query._strOperator;
		String tableName = query._strTableName;
		String columnName = query._strColumnName;
		Object objValue = query._objValue;
		File file = new File("data/" + tableName + 1 + ".class");
		Vector<Hashtable<String, Object>> results = new Vector<Hashtable<String, Object>>();
		int pageNum = 1;

		switch (operator) {
		case "=":

			while (file.exists()) {
				int[] index = { -1, 0 };
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);

				// if not clustering key
				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if (objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
								.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName))) {
							results.add(page.get(i));
						} else if (!(objValue instanceof Polygon)
								&& compareObjects((page.get(i)).get(columnName), objValue) == 0) {
							results.add(page.get(i));
						}
					}
				// if clustering key, check if smaller than or equal max in page.
				else {
					if (compareObjects(page.get(page.size() - 1).get(columnName), objValue) >= 0) {
						index = binarySearchIndex(page, objValue, columnName);
						for (int i = index[0]; i > 0 && i <= index[1]; i++) {
							if (objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
									.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName))) {
								results.add(page.get(i));
							} else if (!(objValue instanceof Polygon)) {
								results.add(page.get(i));
							}
						}

					}
					if (index[1] < n - 1 && index[0] != -1)
						break;
				}

				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case "<":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);
				int[] index;
				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if (compareObjects((page.get(i)).get(columnName), objValue) < 0) {
							results.add(page.get(i));
						}
					}
				else {
					index = binarySearchIndex(page, objValue, columnName);
					if (index[0] != -1) {
						for (int i = 0; i < index[0]; i++) {
							results.add(page.get(i));
						}
					} else {
						for (int i = 0; i < page.size(); i++) {
							if (compareObjects((page.get(i)).get(columnName), objValue) < 0)
								results.add(page.get(i));
						}
					}

					if (index[1] < n - 1 && index[0] != -1)
						break;
				}

				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case ">":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);

				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if (compareObjects((page.get(i)).get(columnName), objValue) > 0) {
							results.add(page.get(i));
						}
					}
				// if clustering key, check if smaller than or equal max in page.
				else {
			//		System.out.println("yaaa");
					if (compareObjects(page.get(page.size() - 1).get(columnName), objValue) > 0) {

						int[] index = binarySearchIndex(page, objValue, columnName);
				//		System.out.println(index[0] + " " + index[1]);
						if (index[0] != -1) {
							for (int i = index[1] + 1; i < page.size(); i++) {

								results.add(page.get(i));

							}
						} else {
							for (int i = 0; i < page.size(); i++) {
								if (compareObjects((page.get(i)).get(columnName), objValue) > 0)
									results.add(page.get(i));
							}

						}

					}
				}
				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case "<=":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);
				int[] index;
				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
								.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))
								|| (objValue instanceof Polygon
										&& compareObjects((page.get(i)).get(columnName), objValue) < 0)) {
							results.add(page.get(i));
						} else if (!(objValue instanceof Polygon)
								&& compareObjects((page.get(i)).get(columnName), objValue) <= 0) {
							results.add(page.get(i));
						}
					}
				else {
					index = binarySearchIndex(page, objValue, columnName);
					if (index[0] != -1) {
						for (int i = 0; i < index[0]; i++) {
							results.add(page.get(i));
						}
						for (int i = index[0]; i <= index[1]; i++) {
							if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
									.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))) {
								results.add(page.get(i));
							} else if (!(objValue instanceof Polygon)) {
								results.add(page.get(i));
							}
						}
					} else {
						for (int i = 0; i < page.size(); i++) {
							if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
									.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))
									|| (objValue instanceof Polygon
											&& compareObjects((page.get(i)).get(columnName), objValue) < 0)) {
								results.add(page.get(i));
							} else if (!(objValue instanceof Polygon)
									&& compareObjects((page.get(i)).get(columnName), objValue) <= 0) {
								results.add(page.get(i));
							}
						}
					}

					if (index[1] < n - 1 && index[0] != -1)
						break;
				}

				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case ">=":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);

				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
								.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))
								|| (objValue instanceof Polygon
										&& compareObjects((page.get(i)).get(columnName), objValue) > 0)) {
							results.add(page.get(i));
						} else if (!(objValue instanceof Polygon)
								&& compareObjects((page.get(i)).get(columnName), objValue) >= 0) {
							results.add(page.get(i));
						}
					}
				// if clustering key, check if smaller than or equal max in page.
				else {
					if (compareObjects(page.get(page.size() - 1).get(columnName), objValue) >= 0) {
						int[] index = binarySearchIndex(page, objValue, columnName);
						if (index[0] != -1) {
							for (int i = index[0]; i <= index[1]; i++) {

								if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
										.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))) {
									results.add(page.get(i));
								} else if (!(objValue instanceof Polygon)) {
									results.add(page.get(i));
								}

							}
							for (int i = index[1] + 1; i < page.size(); i++) {
								results.add(page.get(i));
							}
						} else {
							for (int i = 0; i < page.size(); i++) {
								if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
										.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))
										|| (objValue instanceof Polygon
												&& compareObjects((page.get(i)).get(columnName), objValue) > 0)) {
									results.add(page.get(i));
								} else if (!(objValue instanceof Polygon)
										&& compareObjects((page.get(i)).get(columnName), objValue) >= 0) {
									results.add(page.get(i));
								}
							}

						}

					}
				}
				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case "!=":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);

				for (int i = 0; i < page.size(); i++) {
					if (objValue instanceof Polygon && !new PolygonDB((Polygon) objValue)
							.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName))) {
						results.add(page.get(i));
					} else if (!(objValue instanceof Polygon)
							&& compareObjects((page.get(i)).get(columnName), objValue) != 0) {
						results.add(page.get(i));
					}
				}

				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		}
		return results;

	}

	public Vector<Ref> getUnindexedQueryRef(SQLTerm query, boolean key) {
		String operator = query._strOperator;
		String tableName = query._strTableName;
		String columnName = query._strColumnName;
		Object objValue = query._objValue;
		File file = new File("data/" + tableName + 1 + ".class");
		Vector<Ref> results = new Vector<Ref>();
		int pageNum = 1;

		switch (operator) {
		case "=":

			while (file.exists()) {
				int[] index = { -1, 0 };
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);

				// if not clustering key
				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if (objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
								.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName))) {
							results.add(new Ref(pageNum, i));
						} else if (!(objValue instanceof Polygon)
								&& compareObjects((page.get(i)).get(columnName), objValue) == 0) {
							results.add(new Ref(pageNum, i));
						}
					}
				// if clustering key, check if smaller than or equal max in page.
				else {
					if (compareObjects(page.get(page.size() - 1).get(columnName), objValue) >= 0) {
						index = binarySearchIndex(page, objValue, columnName);
						for (int i = index[0]; i > 0 && i <= index[1]; i++) {
							if (objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
									.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName))) {
								results.add(new Ref(pageNum, i));
							} else if (!(objValue instanceof Polygon)) {
								results.add(new Ref(pageNum, i));
							}
						}

					}
					if (index[1] < n - 1 && index[0] != -1)
						break;
				}

				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case "<":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);
				int[] index;
				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if (compareObjects((page.get(i)).get(columnName), objValue) < 0) {
							results.add(new Ref(pageNum, i));
						}
					}
				else {
					index = binarySearchIndex(page, objValue, columnName);
					if (index[0] != -1) {
						for (int i = 0; i < index[0]; i++) {
							results.add(new Ref(pageNum, i));
						}
					} else {
						for (int i = 0; i < page.size(); i++) {
							if (compareObjects((page.get(i)).get(columnName), objValue) < 0)
								results.add(new Ref(pageNum, i));
						}
					}

					if (index[1] < n - 1 && index[0] != -1)
						break;
				}

				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case ">":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);

				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if (compareObjects((page.get(i)).get(columnName), objValue) > 0) {
							results.add(new Ref(pageNum, i));
						}
					}
				// if clustering key, check if smaller than or equal max in page.
				else {
					if (compareObjects(page.get(page.size() - 1).get(columnName), objValue) > 0) {
						int[] index = binarySearchIndex(page, objValue, columnName);
						if (index[0] != -1) {
							for (int i = index[1] + 1; i < page.size(); i++) {

								results.add(new Ref(pageNum, i));

							}
						} else {
							for (int i = 0; i < page.size(); i++) {
								if (compareObjects((page.get(i)).get(columnName), objValue) > 0)
									results.add(new Ref(pageNum, i));
							}

						}

					}
				}
				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case "<=":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);
				int[] index;
				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
								.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))
								|| (objValue instanceof Polygon
										&& compareObjects((page.get(i)).get(columnName), objValue) < 0)) {
							results.add(new Ref(pageNum, i));
						} else if (!(objValue instanceof Polygon)
								&& compareObjects((page.get(i)).get(columnName), objValue) <= 0) {
							results.add(new Ref(pageNum, i));
						}
					}
				else {
					index = binarySearchIndex(page, objValue, columnName);
					if (index[0] != -1) {
						for (int i = 0; i < index[0]; i++) {
							results.add(new Ref(pageNum, i));
						}
						for (int i = index[0]; i <= index[1]; i++) {
							if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
									.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))) {
								results.add(new Ref(pageNum, i));
							} else if (!(objValue instanceof Polygon)) {
								results.add(new Ref(pageNum, i));
							}
						}
					} else {
						for (int i = 0; i < page.size(); i++) {
							if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
									.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))
									|| (objValue instanceof Polygon
											&& compareObjects((page.get(i)).get(columnName), objValue) < 0)) {
								results.add(new Ref(pageNum, i));
							} else if (!(objValue instanceof Polygon)
									&& compareObjects((page.get(i)).get(columnName), objValue) <= 0) {
								results.add(new Ref(pageNum, i));
							}
						}
					}

					if (index[1] < n - 1 && index[0] != -1)
						break;
				}

				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case ">=":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);

				if (!key)
					for (int i = 0; i < page.size(); i++) {
						if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
								.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))
								|| (objValue instanceof Polygon
										&& compareObjects((page.get(i)).get(columnName), objValue) > 0)) {
							results.add(new Ref(pageNum, i));
						} else if (!(objValue instanceof Polygon)
								&& compareObjects((page.get(i)).get(columnName), objValue) >= 0) {
							results.add(new Ref(pageNum, i));
						}
					}
				// if clustering key, check if smaller than or equal max in page.
				else {
					if (compareObjects(page.get(page.size() - 1).get(columnName), objValue) >= 0) {
						int[] index = binarySearchIndex(page, objValue, columnName);
						if (index[0] != -1) {
							for (int i = index[0]; i <= index[1]; i++) {

								if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
										.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))) {
									results.add(new Ref(pageNum, i));
								} else if (!(objValue instanceof Polygon)) {
									results.add(new Ref(pageNum, i));
								}

							}
							for (int i = index[1] + 1; i < page.size(); i++) {
								results.add(new Ref(pageNum, i));
							}
						} else {
							for (int i = 0; i < page.size(); i++) {
								if ((objValue instanceof Polygon && new PolygonDB((Polygon) objValue)
										.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName)))
										|| (objValue instanceof Polygon
												&& compareObjects((page.get(i)).get(columnName), objValue) > 0)) {
									results.add(new Ref(pageNum, i));
								} else if (!(objValue instanceof Polygon)
										&& compareObjects((page.get(i)).get(columnName), objValue) >= 0) {
									results.add(new Ref(pageNum, i));
								}
							}

						}

					}
				}
				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		case "!=":
			while (file.exists()) {
				Vector<Hashtable<String, Object>> page = deserializePage(tableName, pageNum);

				for (int i = 0; i < page.size(); i++) {
					if (objValue instanceof Polygon && !new PolygonDB((Polygon) objValue)
							.equals((Polygon) ((Hashtable) (page.get(i))).get(columnName))) {
						results.add(new Ref(pageNum, i));
					} else if (!(objValue instanceof Polygon)
							&& compareObjects((page.get(i)).get(columnName), objValue) != 0) {
						results.add(new Ref(pageNum, i));
					}
				}

				serializePage(page, tableName, pageNum);
				pageNum++;
				file = new File("data/" + tableName + pageNum + ".class");
			}
			break;
		}
		return results;

	}

	public int compareObjects(Object myObj, Object hashObj) {
		if (myObj instanceof Integer) {
			return ((Integer) myObj).compareTo(((Integer) hashObj));
		}
		if (myObj instanceof Double) {
			return ((Double) myObj).compareTo(((Double) hashObj));
		}
		if (myObj instanceof String) {
			return ((String) myObj).compareTo(((String) hashObj));
		}
		if (myObj instanceof Date) {
			return ((Date) myObj).compareTo(((Date) hashObj));
		}
		if (myObj instanceof Boolean) {
			return ((Boolean) myObj).compareTo(((Boolean) hashObj));
		}
		// not sure about this
		if (myObj instanceof Polygon) {
			return ((Double) new PolygonDB((Polygon) myObj).getArea())
					.compareTo((Double) new PolygonDB((Polygon) hashObj).getArea());
		}
		return 0;
	}

	// returns the index from which to start the search
	// increments the index as long as objValue is bigger. When it is is smaller, we
	// decrement until it is bigger again.
	public int[] binarySearchIndex(Vector<Hashtable<String, Object>> page, Object objValue, String columnName) {
		if (objValue instanceof Integer) {
			return binarySearch(page, (int) objValue, columnName);
		} else if (objValue instanceof String) {
			return binarySearch(page, (String) objValue, columnName);
		} else if (objValue instanceof Date) {
			return binarySearch(page, (Date) objValue, columnName);
		} else if (objValue instanceof Double) {
			return binarySearch(page, (Double) objValue, columnName);
		} else if (objValue instanceof Polygon) {
			return binarySearch(page, (Polygon) objValue, columnName);
		}
		int[] x = { -1, 0 };
		return x;
	}

	public Vector<Ref> intersection(Vector<Ref> vector1, Vector<Ref> vector2) {
		Vector<Ref> result = new Vector<Ref>();
		if (vector1.size() <= vector2.size()) {
			for (int i = 0; i < vector1.size(); i++) {
				boolean f = false;
				for (int j = 0; j < vector2.size(); j++) {
					if (vector2.get(j).getPage() == vector1.get(i).getPage()
							&& vector2.get(j).getIndexInPage() == vector1.get(i).getIndexInPage())
						f = true;
				}
				if (f)
					result.add(vector1.get(i));
			}
		} else {
			for (int i = 0; i < vector2.size(); i++) {
				boolean f = false;
				for (int j = 0; j < vector1.size(); j++) {
					if ((vector2.get(i).getPage() == vector1.get(j).getPage()
							&& vector2.get(i).getIndexInPage() == vector1.get(j).getIndexInPage()))
						f = true;
				}
				if (f)
					result.add(vector2.get(i));
			}

		}
		return result;

	}

	public Vector union(Vector vector1, Vector vector2) {
		Vector result = new Vector();
		for (int i = 0; i < vector1.size(); i++) {
			result.add(vector1.get(i));
		}
		for (int i = 0; i < vector2.size(); i++) {
			if (!result.contains(vector2.get(i)))
				result.add(vector2.get(i));
		}
		return result;
	}

	public Vector<Ref> xor(Vector<Ref> vector1, Vector<Ref> vector2) {
		Vector<Ref> intersect = intersection(vector1, vector2);
		Vector<Ref> union = union(vector1, vector2);
		Vector<Ref> result = new Vector<Ref>();
		for (int i = 0; i < union.size(); i++) {
			boolean f = false;
			for (int j = 0; j < intersect.size(); j++) {
				if ((intersect.get(j).getPage() == union.get(i).getPage()
						&& intersect.get(j).getIndexInPage() == union.get(i).getIndexInPage()))
					f = true;
			}
			if (!f)
				result.add(union.get(i));
		}
		return result;
	}

	// gets vector of references of an indexed query.
	// problem may occur because query._objValue is object
	// Need to handle RTree indices
	// for equal: puts the reference
	public Vector<Ref> getIndexedQuery(SQLTerm Query, Object tree) {
		// BPTree tree=deserializeBP(Query._strTableName,Query._strColumnName);
		BPTree btree = null;
		RTree rtree = null;
		if (tree instanceof BPTree)
			btree = (BPTree) tree;
		else
			rtree = (RTree) tree;
		Vector<Ref> references = new Vector<Ref>();
		BPTreeLeafNode leaf = null;
		RTreeLeafNode leaf1 = null;
		switch (Query._strOperator) {
		case "=": {
			if (Query._objValue instanceof Integer)
				references = btree.search((Integer) Query._objValue);
			if (Query._objValue instanceof Double)
				references = btree.search((Double) Query._objValue);
			if (Query._objValue instanceof Date)
				references = btree.search((Date) Query._objValue);
			if (Query._objValue instanceof String)
				references = btree.search((String) Query._objValue);
			if (Query._objValue instanceof Polygon) {
				Vector<Ref> ref = rtree.search(((Double) new PolygonDB((Polygon) Query._objValue).getArea()));
                if(ref==null)
                	return null;
				int page = ref.get(0).getPage();
				Vector v = deserializePage(Query._strTableName, page);
				for (int j = 0; j < ref.size(); j++) {
					Hashtable h = (Hashtable) v.get(((Ref) ref.get(j)).getIndexInPage());
					Polygon p = (Polygon) h.get(Query._strColumnName);
					Polygon n = (Polygon) Query._objValue;
					if (new PolygonDB(n).equals(p))
						references.add(ref.get(j));
					if (j + 1 < ref.size() && ((Ref) ref.get(j + 1)).getPage() != page)
						v = deserializePage(Query._strTableName, ((Ref) ref.get(j + 1)).getPage());
				}
			}
			return references;
		}
		case ">": {
			if (Query._objValue instanceof Integer) {
				// if key exists in tree, get the leaf node containing the key and everything
				// after it
				// create method in BPTree to get the leaf node containing the key
				// searchNode(Key)
				// Try to reduce this code

				leaf = (BPTreeLeafNode) btree.getNodeGreater((Integer) Query._objValue);
				// could implement binary search here
				if (leaf != null) {
					for (int i = 0; i < leaf.keys.length; i++)
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) < 0)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					while (leaf.getNext() != null) {
						leaf = leaf.getNext();
						for (int i = 0; i < leaf.keys.length; i++)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));
					}
				}

			} else if (Query._objValue instanceof String) {
				leaf = (BPTreeLeafNode) btree.getNodeGreater((String) Query._objValue);
				// could implement binary search here
				if (leaf != null) {
					for (int i = 0; i < leaf.keys.length; i++)
						if (compareObjects(Query._objValue, leaf.keys[i]) < 0)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					while (leaf.getNext() != null) {
						leaf = leaf.getNext();
						for (int i = 0; i < leaf.keys.length; i++)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					}
				}
			} else if (Query._objValue instanceof Date) {

				leaf = (BPTreeLeafNode) btree.getNodeGreater((Date) Query._objValue);
				// could implement binary search here
				if (leaf != null) {
					for (int i = 0; i < leaf.keys.length; i++)
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) < 0)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					while (leaf.getNext() != null) {
						leaf = leaf.getNext();
						for (int i = 0; i < leaf.keys.length; i++)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));
					}
				}
			} else if (Query._objValue instanceof Double) {
				leaf = (BPTreeLeafNode) btree.getNodeGreater((double) Query._objValue);
				// could implement binary search here
				if (leaf != null) {
					for (int i = 0; i < leaf.keys.length; i++)
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) < 0)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					while (leaf.getNext() != null) {
						leaf = leaf.getNext();
						for (int i = 0; i < leaf.keys.length; i++)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					}
				}
			} else if (Query._objValue instanceof Polygon) {
				leaf1 = (RTreeLeafNode) rtree.getNodeGreater(new PolygonDB((Polygon) Query._objValue).getArea());
				// could implement binary search here
				if (leaf1 != null) {
					for (int i = 0; i < leaf1.keys.length; i++)
						if (leaf1.keys[i] != null
								&& compareObjects((Double) new PolygonDB((Polygon) Query._objValue).getArea(),
										leaf1.keys[i]) < 0)
							for (int j = 0; j < leaf1.getRecord(i).size(); j++)
								references.add((Ref) (leaf1.getRecord(i).get(j)));

					while (leaf1.getNext() != null) {
						leaf1 = leaf1.getNext();
						for (int i = 0; i < leaf1.keys.length; i++)
							for (int j = 0; j < leaf1.getRecord(i).size(); j++)
								references.add((Ref) (leaf1.getRecord(i).get(j)));

					}
				}
			}
			break;
		}
		case ">=": {
			if (Query._objValue instanceof Integer) {
				// if key exists in tree, get the leaf node containing the key and everything
				// after it
				// create method in BPTree to get the leaf node containing the key
				// searchNode(Key)
				// Try to reduce this code

				leaf = (BPTreeLeafNode) btree.getNodeGreaterE((Integer) Query._objValue);
				// could implement binary search here
				if (leaf != null) {
					for (int i = 0; i < leaf.keys.length; i++)
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) <= 0)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					while (leaf.getNext() != null) {
						leaf = leaf.getNext();
						for (int i = 0; i < leaf.keys.length; i++)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));
					}
				}

			} else if (Query._objValue instanceof String) {
				leaf = (BPTreeLeafNode) btree.getNodeGreaterE((String) Query._objValue);
				// could implement binary search here
				if (leaf != null) {
					for (int i = 0; i < leaf.keys.length; i++) {
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) <= 0) {
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));
						}
					}

					while (leaf.getNext() != null) {
						leaf = leaf.getNext();
						for (int i = 0; i < leaf.keys.length; i++)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					}
				}
			} else if (Query._objValue instanceof Date) {

				leaf = (BPTreeLeafNode) btree.getNodeGreaterE((Date) Query._objValue);
				// could implement binary search here
				if (leaf != null) {
					for (int i = 0; i < leaf.keys.length; i++)
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) <= 0)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					while (leaf.getNext() != null) {
						leaf = leaf.getNext();
						for (int i = 0; i < leaf.keys.length; i++)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));
					}
				}
			} else if (Query._objValue instanceof Double) {
				leaf = (BPTreeLeafNode) btree.getNodeGreaterE((double) Query._objValue);
				// could implement binary search here
				if (leaf != null) {
					for (int i = 0; i < leaf.keys.length; i++)
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) <= 0)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					while (leaf.getNext() != null) {
						leaf = leaf.getNext();
						for (int i = 0; i < leaf.keys.length; i++)
							for (int j = 0; j < leaf.getRecord(i).size(); j++)
								references.add((Ref) (leaf.getRecord(i).get(j)));

					}
				}
			} else if (Query._objValue instanceof Polygon) {
				leaf1 = (RTreeLeafNode) rtree.getNodeGreaterE(new PolygonDB((Polygon) Query._objValue).getArea());
				// could implement binary search here
				if (leaf1 != null) {
					for (int i = 0; i < leaf1.keys.length; i++)
						if (leaf1.keys[i] != null
								&& compareObjects((Double) new PolygonDB((Polygon) Query._objValue).getArea(),
										leaf1.keys[i]) < 0) {
							for (int j = 0; j < leaf1.getRecord(i).size(); j++)
								references.add((Ref) (leaf1.getRecord(i).get(j)));
						} else if (leaf1.keys[i] != null
								&& compareObjects((Double) new PolygonDB((Polygon) Query._objValue).getArea(),
										leaf1.keys[i]) == 0) {
							int page = ((Ref) leaf1.getRecord(i).get(0)).getPage();
							Vector v = deserializePage(Query._strTableName, page);
							for (int j = 0; j < leaf1.getRecord(i).size(); j++) {
								Hashtable h = (Hashtable) v.get(((Ref) leaf1.getRecord(i).get(j)).getIndexInPage());
								Polygon p = (Polygon) h.get(Query._strColumnName);
								Polygon n = (Polygon) Query._objValue;
								if (new PolygonDB(n).equals(p))
									references.add((Ref) leaf1.getRecord(i).get(j));
								if (j + 1 < leaf1.getRecord(i).size()
										&& ((Ref) leaf1.getRecord(i).get(j + 1)).getPage() != page)
									v = deserializePage(Query._strTableName,
											((Ref) leaf1.getRecord(i).get(j + 1)).getPage());
							}
						}
					while (leaf1.getNext() != null) {
						leaf1 = leaf1.getNext();
						for (int i = 0; i < leaf1.keys.length; i++)
							for (int j = 0; j < leaf1.getRecord(i).size(); j++)
								references.add((Ref) (leaf1.getRecord(i).get(j)));

					}
				}
			}
			break;
		}
		case "<": {
			// instanceof handled in compareObjects
			boolean less = true;
			if (Query._objValue instanceof Polygon) {
				leaf1 = rtree.getFirstNode();
				while (less) {
					for (int i = 0; i < leaf1.keys.length; i++) {
						if (leaf1.keys[i] != null
								&& compareObjects((Double) new PolygonDB((Polygon) Query._objValue).getArea(),
										leaf1.keys[i]) > 0) {
							// System.out.println(leaf.keys[i]);
							for (int j = 0; j < leaf1.getRecord(i).size(); j++) {
								references.add((Ref) leaf1.getRecord(i).get(j));

							}
						} else {
							less = false;
							break;
						}
					}
					// System.out.println("node");
					if (leaf1.getNext() != null)
						leaf1 = leaf1.getNext();
					else
						break;
				}
			} else {
				leaf = btree.getFirstNode();
				while (less) {
					for (int i = 0; i < leaf.keys.length; i++) {
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) > 0) {
							// System.out.println(leaf.keys[i]);
							for (int j = 0; j < leaf.getRecord(i).size(); j++) {
								references.add((Ref) leaf.getRecord(i).get(j));

							}
						} else {
							less = false;
							break;
						}
					}
					// System.out.println("node");
					if (leaf.getNext() != null)
						leaf = leaf.getNext();
					else
						break;
				}
			}
			// System.out.println(leaf);

			break;
		}
		case "<=": {
			boolean less = true;
			if (Query._objValue instanceof Polygon) {
				leaf1 = rtree.getFirstNode();
				while (less) {
					for (int i = 0; i < leaf1.keys.length; i++) {
						if (leaf1.keys[i] != null
								&& compareObjects((Double) new PolygonDB((Polygon) Query._objValue).getArea(),
										leaf1.keys[i]) > 0) {
							for (int j = 0; j < leaf1.getRecord(i).size(); j++) {
								references.add((Ref) leaf1.getRecord(i).get(j));

							}
						} else if (leaf1.keys[i] != null
								&& compareObjects((Double) new PolygonDB((Polygon) Query._objValue).getArea(),
										leaf1.keys[i]) == 0) {
							int page = ((Ref) leaf1.getRecord(i).get(0)).getPage();
							Vector v = deserializePage(Query._strTableName, page);
							for (int j = 0; j < leaf1.getRecord(i).size(); j++) {
								Hashtable h = (Hashtable) v.get(((Ref) leaf1.getRecord(i).get(j)).getIndexInPage());
								Polygon p = (Polygon) h.get(Query._strColumnName);
								Polygon n = (Polygon) Query._objValue;
								if (new PolygonDB(n).equals(p))
									references.add((Ref) leaf1.getRecord(i).get(j));
								if (j + 1 < leaf1.getRecord(i).size()
										&& ((Ref) leaf1.getRecord(i).get(j + 1)).getPage() != page)
									v = deserializePage(Query._strTableName,
											((Ref) leaf1.getRecord(i).get(j + 1)).getPage());
							}
						} else {
							less = false;
							break;
						}
					}
					if (leaf1.getNext() != null)
						leaf1 = leaf1.getNext();
					else
						break;
				}
			} else {
				leaf = btree.getFirstNode();
				while (less) {
					for (int i = 0; i < leaf.keys.length; i++) {
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) >= 0) {
							for (int j = 0; j < leaf.getRecord(i).size(); j++) {
								references.add((Ref) leaf.getRecord(i).get(j));

							}
						} else {
							less = false;
							break;
						}
					}
					if (leaf.getNext() != null)
						leaf = leaf.getNext();
					else
						break;
				}
			}

			break;
		}
		case "!=": {
			if (Query._objValue instanceof Polygon) {
				leaf1 = rtree.getFirstNode();
				while (true) {

					for (int i = 0; i < leaf1.keys.length; i++) {
						if (leaf1.keys[i] != null
								&& compareObjects((Double) new PolygonDB((Polygon) Query._objValue).getArea(),
										leaf1.keys[i]) != 0)
							for (int j = 0; j < leaf1.getRecord(i).size(); j++) {

								references.add((Ref) leaf1.getRecord(i).get(j));

							}
						else if (leaf1.keys[i] != null) {
							int page = ((Ref) leaf1.getRecord(i).get(0)).getPage();
							Vector v = deserializePage(Query._strTableName, page);
							for (int j = 0; j < leaf1.getRecord(i).size(); j++) {
								Hashtable h = (Hashtable) v.get(((Ref) leaf1.getRecord(i).get(j)).getIndexInPage());
								Polygon p = (Polygon) h.get(Query._strColumnName);
								Polygon n = (Polygon) Query._objValue;
								if (!new PolygonDB(n).equals(p))
									references.add((Ref) leaf1.getRecord(i).get(j));
								if (j + 1 < leaf1.getRecord(i).size()
										&& ((Ref) leaf1.getRecord(i).get(j + 1)).getPage() != page)
									v = deserializePage(Query._strTableName,
											((Ref) leaf1.getRecord(i).get(j + 1)).getPage());

							}

						}

					}

					if (leaf1.getNext() != null)
						leaf1 = leaf1.getNext();
					else
						break;

				}

			} else {
				leaf = btree.getFirstNode();
				while (true) {
					for (int i = 0; i < leaf.keys.length; i++) {
						if (leaf.keys[i] != null && compareObjects(Query._objValue, leaf.keys[i]) != 0)
							for (int j = 0; j < leaf.getRecord(i).size(); j++) {

								references.add((Ref) leaf.getRecord(i).get(j));

							}

					}

					if (leaf.getNext() != null)
						leaf = leaf.getNext();
					else
						break;

				}
			}

			break;
		}

		}
		return references;
	}

	// AND handler
	public Vector<Ref> handleAnd(SQLTerm term1, SQLTerm term2, Vector<Ref> references1, Vector<Ref> references2) {
		// if both have indices
		if (references1.size() != 0 && references2.size() != 0) {
		//	System.out.println("yesssss");
			return intersection(references1, references2);
		}
		// if one doesnt have an index
		else if (references1.size() == 0 && references2.size() != 0) {
			// get size of
			int maxPage = 0;
			for (int i = 0; i < references2.size(); i++) {
				if (references2.get(i).getPage() > maxPage)
					maxPage = references2.get(i).getPage();
			}
			Vector<Vector<Hashtable<String, Object>>> pages = new Vector();
			pages.add(new Vector<Hashtable<String, Object>>());
			for (int i = 0; i < maxPage; i++)
				pages.add(new Vector<Hashtable<String, Object>>());
			// getting all needed pages.
			for (int i = 0; i < references2.size(); i++) {
				if (pages.get(references2.get(i).getPage()).size() == 0) {
					pages.set(references2.get(i).getPage(),
							deserializePage(term1._strTableName, references2.get(i).getPage()));
				}

			}
			Vector<Ref> finalRef = new Vector<Ref>();
			// checks if the condition of the unindexed (in this case term1) is met in the
			// references given by the indexed
			for (int i = 0; i < references2.size(); i++) {
				if (opChecker(term1._strOperator, term1._objValue, pages.get(references2.get(i).getPage())
						.get(references2.get(i).getIndexInPage()).get(term1._strColumnName)))
					finalRef.add(references2.get(i));

			}
			return finalRef;
		} else if (references1.size() != 0 && references2.size() == 0) {
			// get size of
			int maxPage = 0;
			for (int i = 0; i < references1.size(); i++) {
				if (references1.get(i).getPage() > maxPage)
					maxPage = references1.get(i).getPage();
			}
			Vector<Vector<Hashtable<String, Object>>> pages = new Vector();
			pages.add(new Vector<Hashtable<String, Object>>());
			for (int i = 0; i < maxPage; i++)
				pages.add(new Vector<Hashtable<String, Object>>());
			// getting all needed pages.
			for (int i = 0; i < references1.size(); i++) {
				if (pages.get(references1.get(i).getPage()).size() == 0) {
					pages.set(references1.get(i).getPage(),
							deserializePage(term1._strTableName, references1.get(i).getPage()));
				}

			}
			Vector<Ref> finalRef = new Vector<Ref>();
			// checks if the condition of the unindexed (in this case term1) is met in the
			// references given by the indexed
			// maybe put this in another method and use binary search
			for (int i = 0; i < references1.size(); i++) {
				if (opChecker(term2._strOperator, term2._objValue, pages.get(references1.get(i).getPage())
						.get(references1.get(i).getIndexInPage()).get(term2._strColumnName)))
					finalRef.add(references1.get(i));

			}
			return finalRef;
		}

		// both are unindexed
		else if (references1.size() == 0 && references2.size() == 0) {
			// check record by record
			Vector references = new Vector<Ref>();
			for (int i = 1; (new File(term1._strTableName + i + ".class").exists()); i++) {
				Vector<Hashtable<String, Object>> page = deserializePage(term1._strTableName, i);
				for (int j = 0; j < page.size(); j++) {
					if (opChecker(term1._strOperator, term1._objValue, page.get(j).get(term1._strColumnName)))
						if (opChecker(term2._strOperator, term2._objValue, page.get(j).get(term2._strColumnName))) {
							references.add(new Ref(i, j));
						}
				}
			}
			return references;
		}
		// wont be reached
		return null;

	}

	public Vector<Ref> handleOr(SQLTerm term1, SQLTerm term2, Vector<Ref> references1, Vector<Ref> references2) {
		if (references1.size() != 0 && references2.size() != 0) {
			return union(references1, references2);
		}
		if (references1.size() == 0 && references2.size() != 0) {
			Vector<Ref> term1Refs = new Vector<Ref>();
			Vector<Hashtable<String, Object>> page;

			for (int i = 1; (new File(term1._strTableName + i + ".class").exists()); i++) {
				page = deserializePage(term1._strTableName, i);
				for (int j = 0; j < page.size(); j++) {
					if (opChecker(term1._strOperator, term1._objValue, page.get(j).get(term1._strColumnName))) {
						term1Refs.add(new Ref(i, j));
					}
				}
			}
			return union(term1Refs, references2);

		}
		if (references1.size() != 0 && references2.size() == 0) {
			Vector<Ref> term2Refs = new Vector<Ref>();
			Vector<Hashtable<String, Object>> page;

			for (int i = 1; (new File(term2._strTableName + i + ".class").exists()); i++) {
				page = deserializePage(term2._strTableName, i);
				for (int j = 0; j < page.size(); j++) {
					if (opChecker(term2._strOperator, term2._objValue, page.get(j).get(term2._strColumnName))) {
						term2Refs.add(new Ref(i, j));
					}
				}
			}
			return union(term2Refs, references1);

		}
		if (references1.size() == 0 && references2.size() == 0) {
			Vector<Ref> termRefs = new Vector<Ref>();
			Vector<Hashtable<String, Object>> page;

			for (int i = 1; (new File(term1._strTableName + i + ".class").exists()); i++) {
				page = deserializePage(term1._strTableName, i);
				for (int j = 0; j < page.size(); j++) {
					if (opChecker(term2._strOperator, term2._objValue, page.get(j).get(term2._strColumnName))
							|| opChecker(term1._strOperator, term1._objValue, page.get(j).get(term1._strColumnName))) {
						termRefs.add(new Ref(i, j));
					}
				}
			}
			return termRefs;

		}
		return null;

	}

	public Vector<Ref> handleXor(SQLTerm term1, SQLTerm term2, Vector<Ref> references1, Vector<Ref> references2) {
		Vector<Ref> andVector = handleAnd(term1, term2, references1, references2);
		Vector<Ref> orVector = handleOr(term1, term2, references1, references2);
		Vector<Ref> xorVector = new Vector<Ref>();
		for (int i = 0; i < orVector.size(); i++) {
			boolean f = true;
			for (int j = 0; j < andVector.size(); j++) {
				if (orVector.get(i).getPage() == andVector.get(j).getPage()
						&& orVector.get(i).getIndexInPage() == andVector.get(j).getIndexInPage()) {
					f = false;
				}
			}
			if (f)
				xorVector.add(orVector.get(i));
		}
		return xorVector;
	}

	// returns whether a record conforms with the condition or not
	public boolean opChecker(String operator, Object objValue, Object hashValue) {
		switch (operator) {
		case "=":
			if (objValue.equals(hashValue))
				return true;
			return false;
		case "<":
			if (compareObjects(hashValue, objValue) < 0)
				return true;
			return false;
		case "<=":
			if (compareObjects(hashValue, objValue) <= 0)
				return true;
			return false;
		case ">":
			if (compareObjects(hashValue, objValue) > 0)
				return true;
			return false;
		case ">=":
			if (compareObjects(hashValue, objValue) >= 0)
				return true;
			return false;
		case "!=":
			if (compareObjects(hashValue, objValue) != 0)
				return true;
			return false;

		}
		return false;

	}
}
