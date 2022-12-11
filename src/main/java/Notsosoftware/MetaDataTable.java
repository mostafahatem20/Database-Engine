package Notsosoftware;



public class MetaDataTable {

	private MetaDataCol[] metaDataColArray;
	private String tableName;

	public MetaDataTable(String t, int s) {
		tableName = t;
		metaDataColArray = new MetaDataCol[s];
	}

	public MetaDataCol[] getMetaDataColArray() {
		return metaDataColArray;
	}

	public void setMetaDataColArray(MetaDataCol metaCol, int index) {
		this.metaDataColArray[index]=metaCol;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	@Override
	public String toString() {
		String string = "";
		for (int i = 0; i < metaDataColArray.length; i++) {
			string += metaDataColArray[i].toString();
			string += "\n";
		}
		return string;
	}
}
