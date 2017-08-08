package iie.mm.asr;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import iie.mm.dao.DBUtils;
import iie.mm.dao.GlobalConfig;
import iie.mm.datasync.DataSyncPut;

public class AsrHTTP extends AbstractHandler {
	private GlobalConfig gconf = GlobalConfig.getConfig();

	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		if (target == null) {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		} else if (target.equalsIgnoreCase("/callback")) {
			doPost(target, baseRequest, request, response);
		} else if (target.startsWith("/get/")) {
			doGet(target, baseRequest, request, response);
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}
	}

	private void doGet(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        ResourceHandler rh = new ResourceHandler();
        rh.setResourceBase(".");
        rh.handle(target, baseRequest, request, response);
	}

	private void doPost(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		BufferedReader br = request.getReader();
		StringBuffer queryString = new StringBuffer();
		String line = null;
		while ((line = br.readLine()) != null) {
			queryString.append(line + "\n");
		}
		String str = URLDecoder.decode(queryString.toString(), "UTF-8");
//		System.out.print(str);
		if ("sttResult".equals(str.split("=")[0])) {
			JSONObject jo = JSONObject.parseObject(str.split("=")[1]);
			String g_id = jo.get("id")+"";
			if ("health".equals(g_id)){
				AsrHealth.isOK = true;
				return;
			}
			JSONObject sttResult = jo.getJSONObject("sttResult");
			JSONArray items = sttResult.getJSONArray("items");
			queryString.setLength(0);
			for (int i = 0; i < items.size(); i++){
				queryString.append(items.getJSONObject(i).get("content")+",");
			}
			queryString.setCharAt(queryString.length() - 1, '。');
			String content = queryString.toString().replaceAll("\\s","");
//			System.out.println("g_id:" + g_id + " content:" + content);
			boolean upok = false;
			int con = 0;
			while (!upok) {
				upok = DBUtils.updateMpp(content, g_id);
				con++;
				if(con >= 3) {
					upok = true;
					System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [ERR] update mpp " + g_id + " " + content);
				}
			} 
			DataSyncPut.getDataSyncPut().updateMMS(g_id, content);
		} 
		if ("decodeResult".equals(str.split("=")[0])){
			JSONObject jo = JSONObject.parseObject(str.split("=")[1]);
			String g_id = jo.get("id")+"";
			JSONObject sttResult = jo.getJSONObject("decodeResult");
			if ("".equals(sttResult.get("result"))) {
				System.out.println(Thread.currentThread().getName() + gconf.getTime() + " [ERR] ASR err " + g_id);
				DataSyncPut.getDataSyncPut().updateMMS(g_id, "未识别");
			}
		}
		
		okResponse(baseRequest, response, "callback ok");
	}

	private void okResponse(Request baseRequest, HttpServletResponse response, String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getWriter().print(message);
		response.getWriter().flush();
	}

	private void badResponse(Request baseRequest, HttpServletResponse response, String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
		baseRequest.setHandled(true);
		response.getWriter().print(message);
		response.getWriter().flush();
	}
}
