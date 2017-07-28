package iie.databak;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SplitValue;

public class SFileImage implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1l;
	private long fid; // required
	private String dbName; // required
	private String tableName; // required
	private int store_status; // required
	private int rep_nr; // required
	private String digest; // required
	private long record_nr; // required
	private long all_record_nr; // required
	private List<String> sflkeys; // required
	private long length; // required
	private List<Long> ref_files; // required
	private List<SplitValue> values; // required
	private int load_status; // required

	public SFileImage(long fid, String dbName, String tableName,
			int store_status, int rep_nr, String digest, long record_nr,
			long all_record_nr, List<String> sflkeys, long length,
			List<Long> ref_files, List<SplitValue> values, int load_status) {
		this.fid = fid;
		this.dbName = dbName;
		this.tableName = tableName;
		this.store_status = store_status;
		this.rep_nr = rep_nr;
		this.digest = digest;
		this.record_nr = record_nr;
		this.all_record_nr = all_record_nr;
		this.sflkeys = sflkeys;
		this.length = length;
		this.ref_files = ref_files;
		this.values = values;
		this.load_status = load_status;
	}

	public static SFileImage generateSFileImage(SFile sf)
	{
		List<String> sflkeys = new ArrayList<String>();
		if(sf.getLocations() != null)
		{
			for(int i = 0;i<sf.getLocations().size();i++)
			{
				SFileLocation sfl = sf.getLocations().get(i);
				sflkeys.add(generateSflkey(sfl.getLocation(), sfl.getDevid()));
			}
		}
		return new SFileImage(sf.getFid(),sf.getDbName(),sf.getTableName()
				,sf.getStore_status(),sf.getRep_nr(),sf.getDigest(),sf.getRecord_nr()
				,sf.getAll_record_nr(),sflkeys,sf.getLength()
				,sf.getRef_files(),sf.getValues(),sf.getLoad_status());
	}
	
	public static String generateSflkey(String location, String devid)
	{
		String s = location+"_"+devid;
		return s.hashCode()+"";
	}
	
	public long getFid() {
		return fid;
	}

	public void setFid(long fid) {
		this.fid = fid;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public int getStore_status() {
		return store_status;
	}

	public void setStore_status(int store_status) {
		this.store_status = store_status;
	}

	public int getRep_nr() {
		return rep_nr;
	}

	public void setRep_nr(int rep_nr) {
		this.rep_nr = rep_nr;
	}

	public String getDigest() {
		return digest;
	}

	public void setDigest(String digest) {
		this.digest = digest;
	}

	public long getRecord_nr() {
		return record_nr;
	}

	public void setRecord_nr(long record_nr) {
		this.record_nr = record_nr;
	}

	public long getAll_record_nr() {
		return all_record_nr;
	}

	public void setAll_record_nr(long all_record_nr) {
		this.all_record_nr = all_record_nr;
	}

	public List<String> getSflkeys() {
		return sflkeys;
	}

	public void setSflkeys(List<String> sflkeys) {
		this.sflkeys = sflkeys;
	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public List<Long> getRef_files() {
		return ref_files;
	}

	public void setRef_files(List<Long> ref_files) {
		this.ref_files = ref_files;
	}

	public List<SplitValue> getValues() {
		return values;
	}

	public void setValues(List<SplitValue> values) {
		this.values = values;
	}

	public int getLoad_status() {
		return load_status;
	}

	public void setLoad_status(int load_status) {
		this.load_status = load_status;
	}

	
	@Override
	public String toString() {
		return "SFileImage [fid=" + fid + ", dbName=" + dbName + ", tableName="
				+ tableName + ", store_status=" + store_status + ", rep_nr="
				+ rep_nr + ", digest=" + digest + ", record_nr=" + record_nr
				+ ", all_record_nr=" + all_record_nr + ", sflkeys=" + sflkeys
				+ ", length=" + length + ", ref_files=" + ref_files
				+ ", values=" + values + ", load_status=" + load_status + "]";
	}

	
}
