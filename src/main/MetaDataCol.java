package Notsosoftware;



public class MetaDataCol {
	private String tableName;
	private String colName;
	private String colType;
	private Boolean key;
	private Boolean index;
	//number of col??
	
	
	public MetaDataCol(String tableName, String colName, String colType, Boolean key, Boolean index) {
		super();
		this.tableName = tableName;
		this.colName = colName;
		this.colType = colType;
		this.key = key;
		this.index = index;
	}


	public String getTableName() {
		return tableName;
	}


	public void setTableName(String tableName) {
		this.tableName = tableName;
	}


	public String getColName() {
		return colName;
	}


	public void setColName(String colName) {
		this.colName = colName;
	}


	public String getColType() {
		return colType;
	}


	public void setColType(String colType) {
		this.colType = colType;
	}


	public Boolean getKey() {
		return key;
	}


	public void setKey(Boolean key) {
		this.key = key;
	}


	public Boolean getIndex() {
		return index;
	}


	public void setIndex(Boolean index) {
		this.index = index;
	}


	@Override
	public String toString() {
		return tableName+ "," + colName+","+ colType+","+key+","+index;
	}
	
	
	
}
