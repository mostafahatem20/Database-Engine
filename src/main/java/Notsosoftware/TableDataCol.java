package Notsosoftware;



public class TableDataCol {
	private String tableName;
	private String fileName;
	private int size;
	private Object maxKey;
	
	public TableDataCol(String x, String y, int s, Object m)
	{
		tableName = x;
		fileName = y;
		size = s;
		maxKey = m;
	}
	
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
	public Object getMaxKey() {
		return maxKey;
	}
	public void setMaxKey(Object maxKey) {
		this.maxKey = maxKey;
	}
	
}
