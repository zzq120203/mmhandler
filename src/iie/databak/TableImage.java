package iie.databak;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NodeGroup;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

public class TableImage implements Serializable{
	
	 /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
    private String tableName; // required
    private String dbName; // required
    private String schemaName; // required
    private String owner; // required
    private int createTime; // required
    private int lastAccessTime; // required
    private int retention; // required
    private StorageDescriptor sd; // required
    //private String sdKey; // required
    private List<FieldSchema> partitionKeys; // required
    private Map<String,String> parameters; // required
    private String viewOriginalText; // required
    private String viewExpandedText; // required
    private String tableType; // required
    private List<String> ngKeys; // required
    private PrincipalPrivilegeSet privileges; // optional
    private List<Partition> partitions; // optional
    //private List<String> fsKeys; // required
    private List<FieldSchema> fileSplitKeys; // required
    
	public TableImage(String tableName, String dbName, String schemaName,
			String owner, int createTime, int lastAccessTime, int retention,
			StorageDescriptor sd, List<FieldSchema> partitionKeys,
			Map<String, String> parameters, String viewOriginalText,
			String viewExpandedText, String tableType, List<String> ngKeys,
			PrincipalPrivilegeSet privileges, List<Partition> partitions,
			List<FieldSchema> fileSplitKeys) {
		super();
		this.tableName = tableName;
		this.dbName = dbName;
		this.schemaName = schemaName;
		this.owner = owner;
		this.createTime = createTime;
		this.lastAccessTime = lastAccessTime;
		this.retention = retention;
		this.sd = sd;
		this.partitionKeys = partitionKeys;
		this.parameters = parameters;
		this.viewOriginalText = viewOriginalText;
		this.viewExpandedText = viewExpandedText;
		this.tableType = tableType;
		this.ngKeys = ngKeys;
		this.privileges = privileges;
		this.partitions = partitions;
		this.fileSplitKeys = fileSplitKeys;
	}

	public static TableImage generateTableImage(Table tbl)
	{
		List<String> ngKeys = new ArrayList<String>();
		/*String sdKey = tbl.getDbName() + "." + tbl.getTableName() + ".StorageDescriptor";
		List<String> fskeys = new ArrayList<String>();
		for(int i = 0;i<tbl.getFileSplitKeys().size();i++)
		{
			String fskey = "";
			FieldSchema fs = tbl.getFileSplitKeys().get(i);
			fskey = tbl.getDbName() + "." + tbl.getTableName() + ".fs." + fs.getName();
			fskeys.add(fskey);
		}*/
		for(int i = 0;i<tbl.getNodeGroups().size();i++)
		{
			//String ngKey = "";
			NodeGroup ng = tbl.getNodeGroups().get(i);
			//ngKey = tbl.getDbName() + "." + tbl.getTableName() + ".ng." + ng.getNode_group_name();
			ngKeys.add(ng.getNode_group_name());
		}
		return new TableImage(tbl.getTableName(), tbl.getDbName(),
				tbl.getSchemaName(), tbl.getOwner(), tbl.getCreateTime(),
				tbl.getLastAccessTime(), tbl.getRetention(), tbl.getSd(), tbl.getPartitionKeys(), 
				tbl.getParameters(), tbl.getViewOriginalText(), tbl.getViewExpandedText(),
				tbl.getTableType(), ngKeys, tbl.getPrivileges(), tbl.getPartitions(),tbl.getFileSplitKeys());
	}
	
	
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public int getCreateTime() {
		return createTime;
	}

	public void setCreateTime(int createTime) {
		this.createTime = createTime;
	}

	public int getLastAccessTime() {
		return lastAccessTime;
	}

	public void setLastAccessTime(int lastAccessTime) {
		this.lastAccessTime = lastAccessTime;
	}

	public int getRetention() {
		return retention;
	}

	public void setRetention(int retention) {
		this.retention = retention;
	}

	public StorageDescriptor getSd() {
		return sd;
	}

	public void setSd(StorageDescriptor sd) {
		this.sd = sd;
	}

	public List<FieldSchema> getPartitionKeys() {
		return partitionKeys;
	}

	public void setPartitionKeys(List<FieldSchema> partitionKeys) {
		this.partitionKeys = partitionKeys;
	}

	public Map<String, String> getParameters() {
		return parameters;
	}

	public void setParameters(Map<String, String> parameters) {
		this.parameters = parameters;
	}

	public String getViewOriginalText() {
		return viewOriginalText;
	}

	public void setViewOriginalText(String viewOriginalText) {
		this.viewOriginalText = viewOriginalText;
	}

	public String getViewExpandedText() {
		return viewExpandedText;
	}

	public void setViewExpandedText(String viewExpandedText) {
		this.viewExpandedText = viewExpandedText;
	}

	public String getTableType() {
		return tableType;
	}

	public void setTableType(String tableType) {
		this.tableType = tableType;
	}

	public List<String> getNgKeys() {
		return ngKeys;
	}

	public void setNgKeys(List<String> ngKeys) {
		this.ngKeys = ngKeys;
	}

	public PrincipalPrivilegeSet getPrivileges() {
		return privileges;
	}

	public void setPrivileges(PrincipalPrivilegeSet privileges) {
		this.privileges = privileges;
	}

	public List<Partition> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<Partition> partitions) {
		this.partitions = partitions;
	}

	public List<FieldSchema> getFileSplitKeys() {
		return fileSplitKeys;
	}

	public void setFileSplitKeys(List<FieldSchema> fileSplitKeys) {
		this.fileSplitKeys = fileSplitKeys;
	}

	@Override
	public String toString() {
		return "TableImage [dbName=" + dbName + ", tableName=" + tableName + ", schemaName="
				+ schemaName + ", owner=" + owner + ", createTime="
				+ createTime + ", lastAccessTime=" + lastAccessTime + ", retention=" + retention
				+ "]";
	}

	
}
