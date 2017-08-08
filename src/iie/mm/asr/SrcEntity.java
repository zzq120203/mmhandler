package iie.mm.asr;

import iie.mm.dao.GlobalConfig;
import iie.mm.datasync.DataSyncPut;

public class SrcEntity {

	private String id;
    private int flowid;
    private String typecode;
    private String name;
    private String callback;
    private String uri;
    private String engine;
	public SrcEntity(String id, int flowid, String typecode, String name, String callback, String uri, String engine) {
		super();
		this.id = id;
		this.flowid = flowid;
		this.typecode = typecode;
		this.name = name;
		this.callback = callback;
		this.uri = uri;
		this.engine = engine;
	}
	
	public SrcEntity() {
		super();
	}

	public static SrcEntity getSrcEntity(String id, GlobalConfig gconf){
		SrcEntity se = new SrcEntity();
		String key = DataSyncPut.getDataSyncPut().getKg().get(id);
		se.id = id;
		se.flowid = 0;
		if (key.startsWith("a")) {
			se.typecode = "YP";
			se.uri = gconf.getUrl +id + ".wav";
		}
		if (key.startsWith("v")) {
			se.typecode = "SP";
			se.uri = gconf.getUrl +id + ".mp4";
		}
		if (key.startsWith("i")) {
			se.typecode = "TP";
			se.uri = gconf.getUrl +id + ".jpg";
		}
		se.name = id;
		se.callback = gconf.callback;
		se.engine = "opStt";
		return se;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public int getFlowid() {
		return flowid;
	}
	public void setFlowid(int flowid) {
		this.flowid = flowid;
	}
	public String getTypecode() {
		return typecode;
	}
	public void setTypecode(String typecode) {
		this.typecode = typecode;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCallback() {
		return callback;
	}
	public void setCallback(String callback) {
		this.callback = callback;
	}
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getEngine() {
		return engine;
	}
	public void setEngine(String engine) {
		this.engine = engine;
	}
	@Override
	public String toString() {
		return "{id:" + id + ", "
				+ "flowid:" + flowid + ", "
				+ "typecode:" + typecode + ", "
				+ "name:" + name + ", "
				+ "callback:"+ callback + ", "
				+ "uri:" + uri + ", "
				+ "engine:" + engine + "}";
	}
}
