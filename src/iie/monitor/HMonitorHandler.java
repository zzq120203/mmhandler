package iie.monitor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.alibaba.fastjson.JSON;

public class HMonitorHandler extends AbstractHandler {
	
	public class JSONResult implements Serializable {
		private static final long serialVersionUID = 2l;
		private String result;
		private List<Map<String, String>> list;
		
		public JSONResult() {
			setResult("success");
			list = new ArrayList<Map<String, String>>();
		}
		
		public JSONResult(String result, Map<String, String> m) {
			this.setResult(result);
			list = new ArrayList<Map<String, String>>();
			list.add(m);
		}
		
		public synchronized Map<String, String> getSlot(Component c) {
			Map<String, String> rm = null;
			
			if (list == null) {
				list = new ArrayList<Map<String, String>>();
			}
			if (list.size() > 0) {
				for (Map<String, String> m : list) {
					String comp = m.get("component");
					if (comp != null) {
						switch (c) {
						case METASTORE:
							if (comp.equalsIgnoreCase("metastore")) {
								rm = m;
							}
							break;
						case DSERVICE:
							if (comp.equalsIgnoreCase("dservice")) {
								rm = m;
							}
							break;
						case FILE:
							if (comp.equalsIgnoreCase("file")) {
								rm = m;
							}
							break;
						}
					}
				}
			} 
			if (rm == null) {
				Map<String, String> m = new HashMap<String, String>();
				switch (c) {
				case METASTORE:
					m.put("component", "metastore");
					break;
				case DSERVICE:
					m.put("component", "dservice");
					break;
				case FILE:
					m.put("component", "file");
					break;
				}
				list.add(m);
				rm = m;
			}
			return rm;
		}

		public String getResult() {
			return result;
		}

		public void setResult(String result) {
			this.result = result;
		}
		
		public List<Map<String, String>> getList() {
			return list;
		}
	}
	
	public enum Component {
		METASTORE, DSERVICE, FILE,
	}

	private void badResponse(Request baseRequest, HttpServletResponse response,
			String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
		response.getWriter().flush();
	}
	
	private String __getLastRptLine(String dstr) {
		String line = null, line2 = null;
		
		// read the report file
		if (dstr == null || dstr.equalsIgnoreCase("")) {
			Date d = new Date(System.currentTimeMillis());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			dstr = sdf.format(d);
		}
		String str = System.getProperty("DM_REPORT_DIR");
		if (str == null)
			str = System.getenv("DM_REPORT_DIR");
		if (str == null)
			str = System.getProperty("user.dir") + "/sotstore/reports/report-" + dstr;
		else
			str = str + "/sotstore/reports/report-" + dstr;
		File reportFile = new File(str);
		FileReader fr = null;
		if (!reportFile.exists())
			return null;
		try {
			fr = new FileReader(reportFile.getAbsoluteFile());
			BufferedReader br = new BufferedReader(fr);
			while ((line2 = br.readLine()) != null) {
				line = line2;
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return line;
	}
	
	private List<String> __parseLine(String line) {
		List<String> r = new ArrayList<String>();
		
		if (line != null) {
			String[] f = line.split(",");
			if (f != null) {
				for (int i = 0; i < f.length; i++) {
					r.add(f[i]);
				}
			}
		}
		return r;
	}
	
	private JSONResult __packJsonResult(Component c, String dstr) {
		JSONResult jr = new JSONResult();
		
		String line = __getLastRptLine(dstr);
		List<String> f = __parseLine(line);
		if (f.size() <= 0) {
			jr.setResult("fail");
			return jr;
		}
		Map<String, String> m = jr.getSlot(c);
		
		switch (c) {
		case METASTORE:
			if (f.size() > 0) m.put("uptime", f.get(0));
			if (f.size() > 1) m.put("safemode", f.get(1));
			if (f.size() > 2) m.put("total_space", f.get(2));
			if (f.size() > 3) m.put("used_space", f.get(3));
			if (f.size() > 4) m.put("free_space", f.get(4));
			if (f.size() > 5) m.put("total_nodes", f.get(5));
			if (f.size() > 6) m.put("active_nodes", f.get(6));
			if (f.size() > 7) m.put("total_device", f.get(7));
			if (f.size() > 8) m.put("active_device", f.get(8));
			if (f.size() > 9) m.put("fcreate1R", f.get(9));
			if (f.size() > 10) m.put("fcreate1SuccR", f.get(10));
			if (f.size() > 11) m.put("fcreate2R", f.get(11));
			if (f.size() > 12) m.put("fcreate2SuccR", f.get(12));
			if (f.size() > 13) m.put("freopenR", f.get(13));
			if (f.size() > 14) m.put("fgetR", f.get(14));
			if (f.size() > 15) m.put("fcloseR", f.get(15));
			if (f.size() > 16) m.put("freplicateR", f.get(16));
			if (f.size() > 17) m.put("frmlR", f.get(17));
			if (f.size() > 18) m.put("frmpR", f.get(18));
			if (f.size() > 19) m.put("frestoreR", f.get(19));
			if (f.size() > 20) m.put("fdelR", f.get(20));
			if (f.size() > 21) m.put("sflcreateR", f.get(21));
			if (f.size() > 22) m.put("sflonlineR", f.get(22));
			if (f.size() > 23) m.put("sflofflineR", f.get(23));
			if (f.size() > 24) m.put("sflsuspectR", f.get(24));
			if (f.size() > 25) m.put("sfldelR", f.get(25));
			if (f.size() > 26) m.put("fcloseSuccR", f.get(26));
			if (f.size() > 27) m.put("newconn", f.get(27));
			if (f.size() > 28) m.put("delconn", f.get(28));
			if (f.size() > 29) m.put("query", f.get(29));
			if (f.size() > 30) m.put("closeRepLimit", f.get(30));
			if (f.size() > 31) m.put("fixRepLimit", f.get(31));
			if (f.size() > 32) m.put("reqQlen", f.get(32));
			if (f.size() > 33) m.put("cleanQlen", f.get(33));
			if (f.size() > 34) m.put("backupQlen", f.get(34));
			if (f.size() > 35) m.put("totalReportNr", f.get(35));
			if (f.size() > 36) m.put("totalFileRep", f.get(36));
			if (f.size() > 37) m.put("totalFileDel", f.get(37));
			if (f.size() > 38) m.put("toRepNr", f.get(38));
			if (f.size() > 39) m.put("toDeleteNr", f.get(39));
			if (f.size() > 40) m.put("avgReportTs", f.get(40));
			if (f.size() > 41) m.put("timestamp", f.get(41));
			if (f.size() > 42) m.put("totalVerify", f.get(42));
			if (f.size() > 43) m.put("totalFailRep", f.get(43));
			if (f.size() > 44) m.put("totalFailDel", f.get(44));
			if (f.size() > 45) m.put("alonediskStdev", f.get(45));
			if (f.size() > 46) m.put("alonediskAvg", f.get(46));
			if (f.size() > 47) m.put("alonediskFrees", f.get(47));
			if (f.size() > 58) m.put("truetotal", f.get(58));
			if (f.size() > 59) m.put("truefree", f.get(59));
			if (f.size() > 60) m.put("offlinefree", f.get(60));
			if (f.size() > 61) m.put("sharedfree", f.get(61));
			if (f.size() > 62) m.put("replicate", f.get(62));
			if (f.size() > 63) m.put("loadstatus_bad", f.get(63));
			if (f.size() > 64) m.put("l1Ttotal", f.get(64));
			if (f.size() > 65) m.put("l1Tfree", f.get(65));
			if (f.size() > 66) m.put("l1offline", f.get(66));
			if (f.size() > 67) m.put("l2Ttotal", f.get(67));
			if (f.size() > 68) m.put("l2Tfree", f.get(68));
			if (f.size() > 69) m.put("l2offline", f.get(69));
			if (f.size() > 70) m.put("l3Ttotal", f.get(70));
			if (f.size() > 71) m.put("l3Tfree", f.get(71));
			if (f.size() > 72) m.put("l3offline", f.get(72));
			if (f.size() > 73) m.put("l4Ttotal", f.get(73));
			if (f.size() > 74) m.put("l4Tfree", f.get(74));
			if (f.size() > 75) m.put("l4offline", f.get(75));
			if (f.size() > 76) m.put("localQ", f.get(76));
			if (f.size() > 77) m.put("MsgQ", f.get(77));
			if (f.size() > 78) m.put("failedQ", f.get(78));
			break;
		case DSERVICE:
			if (f.size() > 48) m.put("ds.qrep", f.get(48));
			if (f.size() > 49) m.put("ds.hrep", f.get(49));
			if (f.size() > 50) m.put("ds.drep", f.get(50));
			if (f.size() > 51) m.put("ds.qdel", f.get(51));
			if (f.size() > 52) m.put("ds.hdel", f.get(52));
			if (f.size() > 53) m.put("ds.ddel", f.get(53));
			if (f.size() > 54) m.put("ds.tver", f.get(54));
			if (f.size() > 55) m.put("ds.tvyr", f.get(55));
			if (f.size() > 56) m.put("ds.uptime", f.get(56));
			if (f.size() > 57) m.put("ds.load1", f.get(57));
			break;
		case FILE:
			if (f.size() > 0) m.put("uptime", f.get(0));
			m.put("status", "ok");
			break;
		}
		
		return jr;
	}
	
	private void doGetJson(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		String component = request.getParameter("component");
		String dstr = request.getParameter("date");
		JSONResult jr;
		
		if (component == null)
			component = "metastore";
		if (target.equals("/sotstore/json")) {
			if (component.equalsIgnoreCase("metastore")) {
				jr = __packJsonResult(Component.METASTORE, dstr);
			} else if (component.equalsIgnoreCase("dservice")) {
				jr = __packJsonResult(Component.DSERVICE, dstr);
			} else if (component.equalsIgnoreCase("file")) {
				jr = __packJsonResult(Component.FILE, dstr);
			} else {
				throw new IOException("Invalid component name: " + component);
			}
			
			// string response
			response.setContentType("text/plain;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			PrintWriter pw = response.getWriter();
			
			pw.println(JSON.toJSONString(jr));
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid JSON request");
		}
	}
	
	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		try {
			if (target == null) {
				badResponse(baseRequest, response, "#FAIL: target can not be null");
			} else if (target.startsWith("/sotstore/json")) {
				doGetJson(target, baseRequest, request, response);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		} catch (ServletException e) {
			e.printStackTrace();
			throw e;
		} catch (Exception e) {
			e.printStackTrace();
			throw new ServletException(e.getMessage());
		}
	}

}
