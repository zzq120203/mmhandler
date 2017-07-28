package iie.monitor;

import iie.metastore.MetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.hadoop.hive.metastore.api.SplitValue;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory;
import org.apache.hadoop.hive.metastore.tools.PartitionFactory.PartitionInfo;
import org.apache.thrift.TException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;

import com.alibaba.fastjson.JSON;

public class MonitorHandler extends AbstractHandler {

	private HashMap<String, String> cityip = new HashMap<String, String>();
	private HashMap<String, String> mmcityip = new HashMap<String, String>();
	private ConcurrentHashMap<String, Long> uidtime = new ConcurrentHashMap<String, Long>();
	private String targetPath, mmPath, redisPath;
	
	private ConcurrentHashMap<String, MetaStoreClient> climap = new ConcurrentHashMap<String, MetaStoreClient>();
	private int port = 10101;
	
	public MonitorHandler(Map<String, String> addrMap, Map<String, String> mmAddrMap, String targetPath, String mmPath, String redisPath) {
		cityip.putAll(addrMap);
		mmcityip.putAll(mmAddrMap);
		this.targetPath = targetPath;
		this.mmPath = mmPath;
		this.redisPath = redisPath;
		Timer t = new Timer();
		t.schedule(new EvictThread(10 * 1000), 10 * 1000, 10 * 1000);
		
		// connect to all active monitored sites
		for (Map.Entry<String, String> entry : addrMap.entrySet()) {
			System.out.println("Try to connect to metastore [" + entry.getKey() + "] = " + entry.getValue() + ":" + port);
			try {
				MetaStoreClient cli = new MetaStoreClient(entry.getValue(), port);
				climap.put(entry.getKey(), cli);
			} catch (Exception e) {
				System.out.println(" -> FAILED.");
			}
		}
	}
	
	private class EvictThread extends TimerTask {
		private long expireTime;	//单位ms
		
		public EvictThread(long expireTime) {
			this.expireTime = expireTime;
		}

		@Override
		public void run() {
			Set<String> keys = new HashSet<String>();
			for (Map.Entry<String, Long> en : uidtime.entrySet()) {
				if (System.currentTimeMillis() - en.getValue() > expireTime) {
					keys.add(en.getKey());
					if (!(en.getKey().equalsIgnoreCase("") || en.getKey().contains("*") || en.getKey().contains("."))) {
						try {
							runCmd("rm -rf datacount/" + en.getKey());
							System.out.println("rm -rf datacount/" + en.getKey());
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}
			for(String s : keys) {
				uidtime.remove(s);
			}
		}
	}
	
	private void badResponse(Request baseRequest, HttpServletResponse response,
			String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
		response.getWriter().flush();
	}

	private void doDataCount(String target, Request baseRequest,
		HttpServletRequest request, HttpServletResponse response)throws IOException, ServletException {
		String city = request.getParameter("city");
		String date = request.getParameter("date");
		String id = request.getParameter("id");
		if (target.equals("/monitor/main.html")) {
			if (city != null && date != null && id != null &&
					!city.equalsIgnoreCase("") &&
					!date.equalsIgnoreCase("") && 
					!id.equalsIgnoreCase("")) {
				uidtime.put(id, System.currentTimeMillis());
				String name = "report-" + date;
				String cmd = "cd monitor;"
						+ "expect data.exp " + cityip.get(city) + " " + targetPath + "/" + name + " " + id + "/" + city + "." + name + ";"
						+ " ./doplot.sh " + id + "/" + city + "." + name + " " + id + "/";
				System.out.println(cmd);
				String error = runCmd(cmd);
				if (error != null) {
					badResponse(baseRequest, response, "#FAIL: "+error);
					return;
				}
				if (mmcityip.get(city) != null) {
					cmd = "cd monitor;"
							+ "expect data.exp " + mmcityip.get(city) + " " + mmPath + "/sysinfo-" + date + " " + id + "/" + city + ".sysinfo-" + date + ";"
							+ " ./doplot2.sh " + id + "/ " + id  + "/" + city + ".sysinfo-" + date;
					System.out.println(cmd);
					error = runCmd(cmd);
					if (error != null) {
						badResponse(baseRequest, response, "#FAIL: "+error);
						return;
					}
				} else {
					cmd = "cd monitor;"
							+ "rm -rf " + id + "/mms_*.png";
					System.out.println(cmd);
					runCmd(cmd);
				}
				{
					cmd = "cd monitor;" 
							+ "expect data.exp " + cityip.get(city) + " " + redisPath + "/redisinfo-" + date + " " + id + "/" + city + ".redisinfo-" + date + ";"
							+ " ./doplot3.sh " + id + "/ " + id + "/" + city + ".redisinfo-" + date;
					System.out.println(cmd);
					error = runCmd(cmd);
					if (error != null) {
						// ignore any error here
						System.out.println("redis info plot failed， ignore it!");
						cmd = "cd monitor;"
								+ "rm -rf " + id + "/redis_*.png";
						System.out.println(cmd);
						runCmd(cmd);
					}
				}
			}
		}
		ResourceHandler rh = new ResourceHandler();
		rh.setResourceBase(".");
		rh.handle(target, baseRequest, request, response);
	}

	private String runCmd(String cmd) throws IOException {
		Process p = Runtime.getRuntime().exec(new String[] { "/bin/bash", "-c", cmd });
		try {
			InputStream err = p.getErrorStream();
			InputStreamReader isr = new InputStreamReader(err);
			BufferedReader br = new BufferedReader(isr);
			String error = null;
			String line = null;
			while ((line = br.readLine()) != null) {
				System.out.println(line);
				error += line + "\n";
			}
			int exitVal = p.waitFor();
			if (exitVal > 0)
				return error;
			else
				return null;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private List<SFile> __get_table_file_by_date(MetaStoreClient cli, Table t, long date) {
		List<SFile> rf = new ArrayList<SFile>();
		List<SplitValue> lsv = new ArrayList<SplitValue>();
		
		if (t.getFileSplitKeysSize() > 0) {
			int maxv = 0;
			List<PartitionInfo> allpis = PartitionFactory.PartitionInfo.getPartitionInfo(t.getFileSplitKeys());
			
			for (PartitionInfo pi : allpis) {
				if (maxv < pi.getP_version())
					maxv = pi.getP_version();
			}
			List<List<PartitionInfo>> vpis = new ArrayList<List<PartitionInfo>>();
			for (int i = 0; i <= maxv; i++) {
				List<PartitionInfo> lpi = new ArrayList<PartitionInfo>();
				vpis.add(lpi);
			}
			for (PartitionInfo pi : allpis) {
				vpis.get(pi.getP_version()).add(pi);
			}
			// ok, we get versioned PIs; for each version, we generate a LSV and call filterTable
			for (int i = 0; i <= maxv; i++) {
				// BUG: in our lv13 demo systems, versions leaks, so we have to ignore some nonexist versions
				if (vpis.get(i).size() <= 0) {
					System.out.println("Metadata corrupted, version " + i + " leaks.");
					continue;
				}
				if (vpis.get(i).get(0).getP_type() != PartitionFactory.PartitionType.interval)
					continue;
				lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)date).toString(), vpis.get(i).get(0).getP_version()));
				lsv.add(new SplitValue(vpis.get(i).get(0).getP_col(), 1, ((Long)(date + Integer.parseInt(vpis.get(i).get(0).getArgs().get(1)) * 3600)).toString(), vpis.get(i).get(0).getP_version()));
				try {
					List<SFile> files = cli.client.filterTableFiles(t.getDbName(), t.getTableName(), lsv);
					System.out.println("Got Table " + t.getTableName() + " LSV: " + lsv + " Hit " + files.size());
					if (files.size() > 0)
						rf.addAll(files);
				} catch (Exception e) {
					e.printStackTrace();
				}
				lsv.clear();
			}
		}
		return rf;
	}
	
	// db.table -> file_nr
	private String __gen_csv1_1(HashMap<String, List<SFile>> map) {
		String r = "db_table,file_nr\n";
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() == null)
				r += e.getKey() + ",0\n";
			else
				r += e.getKey() + "," + e.getValue().size() + "\n";
		}
		
		return r;
	}
	
	// db.table -> rec_nr
	private String __gen_csv1_2(HashMap<String, List<SFile>> map) {
		String r = "db_table,rec_nr\n";
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() == null)
				r += e.getKey() + ",0\n";
			else {
				long rec_nr = 0;
				for (SFile f : e.getValue()) {
					rec_nr += f.getRecord_nr();
				}
				r += e.getKey() + "," + rec_nr + "\n";
			}
		}
		
		return r;
	}
	
	// db.table -> space
	private String __gen_csv2_1(HashMap<String, List<SFile>> map) {
		String r = "db_table,space\n";
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet())	{
			if (e.getValue() == null)
				r += e.getKey() + ",0\n";
			else {
				long space = 0;
				for (SFile f : e.getValue()) {
					space += f.getLength();
				}
				r += e.getKey() + "," + space + "\n";
			}
		}
		
		return r;
	}
	
	private class _MSNR {
		long master_nr;
		long slave_nr;
		
		public _MSNR() {
			master_nr = 0;
			slave_nr = 0;
		}
	}
	// node -> master_file_nr,slave_file_nr
	private String __gen_csv3_1(HashMap<String, List<SFile>> map) {
		String r = "node,master_file_nr,slave_file_nr\n";
		HashMap<String, _MSNR> nrmap = new HashMap<String, _MSNR>();
		_MSNR _nr;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					switch (f.getLocationsSize()) {
					case 1:
						// master copy
						_nr = nrmap.get(f.getLocations().get(0).getNode_name());
						if (_nr == null)
							_nr = new _MSNR();
						_nr.master_nr++;
						nrmap.put(f.getLocations().get(0).getNode_name(), _nr);
						break;
					case 0:
						// no copy
						break;
					default:
						// master and slave(s)
						_nr = nrmap.get(f.getLocations().get(0).getNode_name());
						if (_nr == null)
							_nr = new _MSNR();
						_nr.master_nr++;
						nrmap.put(f.getLocations().get(0).getNode_name(), _nr);
						for (int j = 1; j < f.getLocationsSize(); j++) {
							_nr = nrmap.get(f.getLocations().get(j).getNode_name());
							if (_nr == null)
								_nr = new _MSNR();
							_nr.slave_nr++;
							nrmap.put(f.getLocations().get(j).getNode_name(), _nr);
						}
						break;
					}
				}
			}
		}
		
		for (Map.Entry<String, _MSNR> e : nrmap.entrySet()) {
			r += e.getKey() + "," + e.getValue().master_nr + "," + e.getValue().slave_nr + "\n";
		}
		
		return r;
	}
	
	// node -> master_file_rec,slave_file_rec
	private String __gen_csv3_2(HashMap<String, List<SFile>> map) {
		String r = "node,master_file_rec,slave_file_rec\n";
		HashMap<String, _MSNR> nrmap = new HashMap<String, _MSNR>();
		_MSNR _nr;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					switch (f.getLocationsSize()) {
					case 1:
						// master copy
						_nr = nrmap.get(f.getLocations().get(0).getNode_name());
						if (_nr == null)
							_nr = new _MSNR();
						_nr.master_nr += f.getRecord_nr();
						nrmap.put(f.getLocations().get(0).getNode_name(), _nr);
						break;
					case 0:
						// no copy
						break;
					default:
						// master and slave(s)
						_nr = nrmap.get(f.getLocations().get(0).getNode_name());
						if (_nr == null)
							_nr = new _MSNR();
						_nr.master_nr += f.getRecord_nr();
						nrmap.put(f.getLocations().get(0).getNode_name(), _nr);
						for (int j = 1; j < f.getLocationsSize(); j++) {
							_nr = nrmap.get(f.getLocations().get(j).getNode_name());
							if (_nr == null)
								_nr = new _MSNR();
							_nr.slave_nr += f.getRecord_nr();
							nrmap.put(f.getLocations().get(j).getNode_name(), _nr);
						}
						break;
					}
				}
			}
		}
		
		for (Map.Entry<String, _MSNR> e : nrmap.entrySet()) {
			r += e.getKey() + "," + e.getValue().master_nr + "," + e.getValue().slave_nr + "\n";
		}
		
		return r;
	}
	
	// node -> master_file_space,slave_file_space
	private String __gen_csv3_3(HashMap<String, List<SFile>> map) {
		String r = "node,master_file_space,slave_file_space\n";
		HashMap<String, _MSNR> nrmap = new HashMap<String, _MSNR>();
		_MSNR _nr;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					switch (f.getLocationsSize()) {
					case 1:
						// master copy
						_nr = nrmap.get(f.getLocations().get(0).getNode_name());
						if (_nr == null)
							_nr = new _MSNR();
						_nr.master_nr += f.getLength();
						nrmap.put(f.getLocations().get(0).getNode_name(), _nr);
						break;
					case 0:
						// no copy
						break;
					default:
						// master and slave(s)
						_nr = nrmap.get(f.getLocations().get(0).getNode_name());
						if (_nr == null)
							_nr = new _MSNR();
						_nr.master_nr += f.getLength();
						nrmap.put(f.getLocations().get(0).getNode_name(), _nr);
						for (int j = 1; j < f.getLocationsSize(); j++) {
							_nr = nrmap.get(f.getLocations().get(j).getNode_name());
							if (_nr == null)
								_nr = new _MSNR();
							_nr.slave_nr += f.getLength();
							nrmap.put(f.getLocations().get(j).getNode_name(), _nr);
						}
						break;
					}
				}
			}
		}
		
		for (Map.Entry<String, _MSNR> e : nrmap.entrySet()) {
			r += e.getKey() + "," + e.getValue().master_nr + "," + e.getValue().slave_nr + "\n";
		}
		
		return r;
	}
	
	// node -> space
	private String __gen_csv4_1(HashMap<String, List<SFile>> map) {
		String r = "node,space\n";
		HashMap<String, Long> smap = new HashMap<String, Long>();
		Long _space;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					if (f.getLocations() != null) {
						for (SFileLocation sfl : f.getLocations()) {
							_space = smap.get(sfl.getNode_name());
							if (_space == null)
								_space = 0L;
							_space += f.getLength();
							smap.put(sfl.getNode_name(), _space);
						}
					}
				}
			}
		}
		
		for (Map.Entry<String, Long> e : smap.entrySet()) {
			r += e.getKey() + "," + e.getValue() + "\n";
		}
		
		return r;
	}
	
	private class _FSNR {
		Long s[];
		public _FSNR() {
			s = new Long[5];
			for (int i = 0; i < s.length; i++) {
				s[i] = 0L;
			}
		}
	}
	// db.table + file_status[0-4] -> file_nr 
	private String __gen_csv5_1(HashMap<String, List<SFile>> map) {
		String r = "db_table,fs0,fs1,fs2,fs3,fs4\n";
		HashMap<String, _FSNR> nrmap = new HashMap<String, _FSNR>();
		_FSNR _nr;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					_nr = nrmap.get(f.getDbName() + "." + f.getTableName());
					if (_nr == null)
						_nr = new _FSNR();
					_nr.s[f.getStore_status()]++;
					nrmap.put(f.getDbName() + "." + f.getTableName(), _nr);
				}
			}
		}
		
		for (Map.Entry<String, _FSNR> e : nrmap.entrySet()) {
			r += e.getKey() + ",";
			for (int i = 0; i < 5; i++) {
				r += e.getValue().s[i] + ",";
			}
			r += "\n";
		}
		
		return r;
	}
	
	// devid -> rec_nr
	private String __gen_csv6_1(HashMap<String, List<SFile>> map) {
		String r = "node,devid,rec_nr\n";
		HashMap<String, Long> nrmap = new HashMap<String, Long>();
		Long _nr;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					if (f.getLocations() != null) {
						for (SFileLocation sfl : f.getLocations()) {
							_nr = nrmap.get(sfl.getNode_name() + "," + sfl.getDevid());
							if (_nr == null)
								_nr = 0L;
							_nr += f.getRecord_nr();
							nrmap.put(sfl.getNode_name() + "," + sfl.getDevid(), _nr);
						}
					}
				}
			}
		}
		
		for (Map.Entry<String, Long> e : nrmap.entrySet()) {
			r += e.getKey() + "," + e.getValue() + "\n";
		}
		
		return r;
	}
	
	// devid -> space
	private String __gen_csv6_2(HashMap<String, List<SFile>> map) {
		String r = "node,devid,space\n";
		HashMap<String, Long> smap = new HashMap<String, Long>();
		Long space;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					if (f.getLocations() != null) {
						for (SFileLocation sfl : f.getLocations()) {
							space = smap.get(sfl.getNode_name() + "," + sfl.getDevid());
							if (space == null)
								space = 0L;
							space += f.getLength();
							smap.put(sfl.getNode_name() + "," + sfl.getDevid(), space);
						}
					}
				}
			}
		}
		
		for (Map.Entry<String, Long> e : smap.entrySet()) {
			r += e.getKey() + "," + e.getValue() + "\n";
		}
		
		return r;
	}
	
	// node + devs[] -> rec_nr
	private String __gen_csv6_3(HashMap<String, List<SFile>> map) {
		String h = "node", r = "\n";
		HashMap<String, HashMap<String, Long>> nrmap = new HashMap<String, HashMap<String, Long>>();
		HashMap<String, Long> _nr;
		Long _n;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					if (f.getLocations() != null) {
						for (SFileLocation sfl : f.getLocations()) {
							_nr = nrmap.get(sfl.getNode_name());
							if (_nr == null)
								_nr = new HashMap<String, Long>();
							_n = _nr.get(sfl.getDevid());
							if (_n == null)
								_n = 0L;
							_n += f.getRecord_nr();
							_nr.put(sfl.getDevid(), _n);
							nrmap.put(sfl.getNode_name(), _nr);
						}
					}
				}
			}
		}
		
		_n = 0L;
		for (Map.Entry<String, HashMap<String, Long>> e : nrmap.entrySet()) {
			if (_n < e.getValue().size())
				_n = (long) e.getValue().size();
		}
		for (int i = 0; i < _n; i++) {
			h += ",d" + i;
		}
		for (Map.Entry<String, HashMap<String, Long>> e : nrmap.entrySet()) {
			int j = 0;
			r += e.getKey();
			for (Map.Entry<String, Long> e2 : e.getValue().entrySet()) {
				r += "," + e2.getValue();
				j++;
			}
			for (; j < _n; j++)
				r += ",0";
			r += "\n";
		}
		
		return h + r;
	}
	
	// node + devs[] -> space
	private String __gen_csv6_4(HashMap<String, List<SFile>> map) {
		String h = "node", r = "\n";
		HashMap<String, HashMap<String, Long>> nrmap = new HashMap<String, HashMap<String, Long>>();
		HashMap<String, Long> _nr;
		Long _n;
		
		for (Map.Entry<String, List<SFile>> e : map.entrySet()) {
			if (e.getValue() != null) {
				for (SFile f : e.getValue()) {
					if (f.getLocations() != null) {
						for (SFileLocation sfl : f.getLocations()) {
							_nr = nrmap.get(sfl.getNode_name());
							if (_nr == null)
								_nr = new HashMap<String, Long>();
							_n = _nr.get(sfl.getDevid());
							if (_n == null)
								_n = 0L;
							_n += f.getLength();
							_nr.put(sfl.getDevid(), _n);
							nrmap.put(sfl.getNode_name(), _nr);
						}
					}
				}
			}
		}
		
		_n = 0L;
		for (Map.Entry<String, HashMap<String, Long>> e : nrmap.entrySet()) {
			if (_n < e.getValue().size())
				_n = (long) e.getValue().size();
		}
		for (int i = 0; i < _n; i++) {
			h += ",d" + i;
		}
		for (Map.Entry<String, HashMap<String, Long>> e : nrmap.entrySet()) {
			int j = 0;
			r += e.getKey();
			for (Map.Entry<String, Long> e2 : e.getValue().entrySet()) {
				r += "," + e2.getValue();
				j++;
			}
			for (; j < _n; j++)
				r += ",0";
			r += "\n";
		}
		
		return h + r;
	}

	private void doGetData(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		String city = request.getParameter("city");
		String date = request.getParameter("date");
		String id = request.getParameter("id");
		String hours = request.getParameter("hours");
		String snhours = request.getParameter("nhours");
		String indb = request.getParameter("db");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		HashMap<String, List<SFile>> map = new HashMap<String, List<SFile>>();
		
		if (target.equals("/monitor/getdata.html")) {
			if (city != null && date != null && id != null &&
					!city.equalsIgnoreCase("") &&
					!date.equalsIgnoreCase("") && 
					!id.equalsIgnoreCase("")) {
				MetaStoreClient cli = climap.get(city);
				if (cli == null) {
					// try to reconnect it.
					try {
						cli = new MetaStoreClient(cityip.get(city), port);
						climap.put(city, cli);
					} catch (Exception e) {
						e.printStackTrace();
						badResponse(baseRequest, response, e.getMessage());
					}
				}

				Date d;
				try {
					d = df.parse(date);
				} catch (ParseException e1) {
					throw new IOException("Invalid date str: " + e1.getMessage());
				}
				try {
					// find all the databases
					List<Database> dbs = cli.client.get_all_attributions();
					if (indb != null) {
						dbs.clear();
						dbs.add(new Database(indb, null, null, null));
					}
					
					for (Database db : dbs) {
						List<String> tbnames = cli.client.getAllTables(db.getName());
						
						for (String tbname : tbnames) {
							Table tab = cli.client.getTable(db.getName(), tbname);
							List<SFile> tfiles = new ArrayList<SFile>();
							Long nhours = 24L, nhour = 0L;
							
							// find files that create in specified date
							try {
								if (hours != null) {
									nhour = Long.parseLong(hours);
									nhours = 1L;
								}
								if (snhours != null) {
									nhours = Long.parseLong(snhours);
								}
							} catch (Exception e) {
							}
							for (int i = 0; i < nhours; i++) {
								List<SFile> files = __get_table_file_by_date(cli, tab, d.getTime() / 1000 + 
										(i + nhour) * 3600);
								if (files != null)
									tfiles.addAll(files);
							}
							map.put(db.getName() + "." + tab.getTableName(), tfiles);
						}
					}
				} catch (MetaException e) {
					e.printStackTrace();
				} catch (TException e) {
					e.printStackTrace();
				}

				// finally, return getdata.html page to user and to many ./id/city.csv file
				String fpath = "monitor" + "/" + id + "/";
				// for page1.1: db.table -> file_nr
				{
					String r = __gen_csv1_1(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page1.1.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page1.2: db.table -> rec_nr
				{
					String r = __gen_csv1_2(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page1.2.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page2.1: db.table -> space
				{
					String r = __gen_csv2_1(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page2.1.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page3.1: node -> master_file_nr,slave_file_nr
				{
					String r = __gen_csv3_1(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page3.1.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page3.2: node -> master_file_rec,slave_file_rec
				{
					String r = __gen_csv3_2(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page3.2.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page3.3: node -> master_file_space,slave_file_space
				{
					String r = __gen_csv3_3(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page3.3.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page4.1: node -> space
				{
					String r = __gen_csv4_1(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page4.1.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page5.1: db_table -> file_status -> file_nr 
				{
					String r = __gen_csv5_1(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page5.1.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page6.1: devid -> rec_nr
				{
					String r = __gen_csv6_1(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page6.1.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page6.2: devid -> space
				{
					String r = __gen_csv6_2(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page6.2.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page6.3: node+devs[] -> rec_nr
				{
					String r = __gen_csv6_3(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page6.3.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				// for page6.4: node+devs[] -> space
				{
					String r = __gen_csv6_4(map);
					FileWriter fw = null;
					try {
						File f = new File(fpath + "page6.4.csv");
						if (!f.exists())
							f.createNewFile();
						fw = new FileWriter(f, false);
						BufferedWriter w = new BufferedWriter(fw);
						w.write(r);
						w.close();
						fw.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		ResourceHandler rh = new ResourceHandler();
		rh.setResourceBase(".");
		rh.handle(target, baseRequest, request, response);
	}

	@Override
	public void handle(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		if (target == null) {
			badResponse(baseRequest, response, "#FAIL:taget can not be null");
		} else if (target.startsWith("/monitor/getdata")) {
			doGetData(target, baseRequest, request, response);
		} else if (target.startsWith("/monitor/")) {
			doDataCount(target, baseRequest, request, response);
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}
	}
}
