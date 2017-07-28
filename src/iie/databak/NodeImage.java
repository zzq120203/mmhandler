package iie.databak;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

public class NodeImage implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//private List<FieldSchema> cols; // required
	  private String location; // required
	  private String inputFormat; // required
	  private String outputFormat; // required
	  private boolean compressed; // required
	  private int numBuckets; // required
	  private SerDeInfo serdeInfo; // required
	  private List<String> bucketCols; // required
	  private List<Order> sortCols; // required
	  private Map<String,String> parameters; // required
	  private SkewedInfo skewedInfo; // optional
	  private boolean storedAsSubDirectories; // optional

	public NodeImage(String location, String inputFormat,
			String outputFormat, boolean compressed, int numBuckets,
			SerDeInfo serdeInfo, List<String> bucketCols, List<Order> sortCols,
			Map<String, String> parameters, SkewedInfo skewedInfo,
			boolean storedAsSubDirectories) {
		super();
		this.location = location;
		this.inputFormat = inputFormat;
		this.outputFormat = outputFormat;
		this.compressed = compressed;
		this.numBuckets = numBuckets;
		this.serdeInfo = serdeInfo;
		this.bucketCols = bucketCols;
		this.sortCols = sortCols;
		this.parameters = parameters;
		this.skewedInfo = skewedInfo;
		this.storedAsSubDirectories = storedAsSubDirectories;
	}

	public static NodeImage generateStorageDescriptor(StorageDescriptor sd)
	{
		List<String> sdlkeys = new ArrayList<String>();
		for(int i = 0;i<sd.getCols().size();i++)
		{
			FieldSchema fs = sd.getCols().get(i);
			sdlkeys.add(fs.getName());
		}
		return new NodeImage(sd.getLocation(), sd.getInputFormat(), 
				sd.getOutputFormat(), sd.isCompressed(), sd.getNumBuckets(), 
				sd.getSerdeInfo(), sd.getBucketCols(),sd.getSortCols(),sd.getParameters(), 
				sd.getSkewedInfo(), sd.isStoredAsSubDirectories());
	}
	
	

	
	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getInputFormat() {
		return inputFormat;
	}

	public void setInputFormat(String inputFormat) {
		this.inputFormat = inputFormat;
	}

	public String getOutputFormat() {
		return outputFormat;
	}

	public void setOutputFormat(String outputFormat) {
		this.outputFormat = outputFormat;
	}

	public boolean isCompressed() {
		return compressed;
	}

	public void setCompressed(boolean compressed) {
		this.compressed = compressed;
	}

	public int getNumBuckets() {
		return numBuckets;
	}

	public void setNumBuckets(int numBuckets) {
		this.numBuckets = numBuckets;
	}

	public SerDeInfo getSerdeInfo() {
		return serdeInfo;
	}

	public void setSerdeInfo(SerDeInfo serdeInfo) {
		this.serdeInfo = serdeInfo;
	}

	public List<String> getBucketCols() {
		return bucketCols;
	}

	public void setBucketCols(List<String> bucketCols) {
		this.bucketCols = bucketCols;
	}

	public List<Order> getSortCols() {
		return sortCols;
	}

	public void setSortCols(List<Order> sortCols) {
		this.sortCols = sortCols;
	}

	public Map<String, String> getParameters() {
		return parameters;
	}

	public void setParameters(Map<String, String> parameters) {
		this.parameters = parameters;
	}

	public SkewedInfo getSkewedInfo() {
		return skewedInfo;
	}

	public void setSkewedInfo(SkewedInfo skewedInfo) {
		this.skewedInfo = skewedInfo;
	}

	public boolean isStoredAsSubDirectories() {
		return storedAsSubDirectories;
	}

	public void setStoredAsSubDirectories(boolean storedAsSubDirectories) {
		this.storedAsSubDirectories = storedAsSubDirectories;
	}

	@Override
	public String toString() {
		return "StorageDescriptor [location=" + location + ", inputFormat=" + inputFormat + ", outputFormat="
				+ outputFormat + ", compressed=" + compressed + ", numBuckets="
				+ numBuckets  + "]";
	}

	
}
