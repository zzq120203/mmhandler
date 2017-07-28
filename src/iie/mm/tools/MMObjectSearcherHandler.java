package iie.mm.tools;

import iie.mm.client.ClientAPI;
import iie.mm.client.Feature;
import iie.mm.client.ImagePHash;
import iie.mm.client.Feature.FeatureType;
import iie.mm.client.ResultSet;
import iie.mm.client.ResultSet.Result;

import java.awt.image.BufferedImage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ResourceHandler;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

public class MMObjectSearcherHandler extends AbstractHandler {
	private SearcherConf conf;
	private ClientAPI ca = new ClientAPI();
	private static Random rand = new Random();
	
	public MMObjectSearcherHandler(SearcherConf conf) throws Exception {
		this.conf = conf;
		ca.init(conf.serverUri, "TEST");
		Jedis jedis = ca.getPc().getRpL1().getResource();
		if (jedis != null) {
			try {
				Set<Tuple> active = jedis.zrangeWithScores("mm.active.http", 0, -1);
				if (active != null && active.size() > 0) {
					for (Tuple t : active) {
						// translate ServerName to IP address
						String ipport = jedis.hget("mm.dns", t.getElement());

						// update server ID->Name map
						if (ipport == null) {
							SearcherConf.hservers.put((long)t.getScore(), 
									t.getElement());
							ipport = t.getElement();
						} else
							SearcherConf.hservers.put((long)t.getScore(), ipport);

						System.out.println("Got HTTP Server " + (long)t.getScore() + 
								" " + ipport);
					}
				}
			} finally {
				ca.getPc().getRpL1().putInstance(jedis);
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
	
	private void notFoundResponse(Request baseRequest, HttpServletResponse response, 
			String message) throws IOException {
		response.setContentType("text/html;charset=utf-8");
		response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		baseRequest.setHandled(true);
		response.getWriter().println(message);
		response.getWriter().flush();
	}
	
	private void okResponse(Request baseRequest, HttpServletResponse response, 
			byte[] content) throws IOException {
		// FIXME: text/image/audio/video/application/thumbnail/other
		response.setStatus(HttpServletResponse.SC_OK);
		baseRequest.setHandled(true);
		response.getOutputStream().write(content);
		response.getOutputStream().flush();
	}
	
	public static BufferedImage readImage(byte[] b, int offset, int length) 
			throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(b, offset, length);    
		BufferedImage image = ImageIO.read(in);     
		return image;
	}
	
	public static BufferedImage readImage(byte[] b) throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(b);    
		BufferedImage image = ImageIO.read(in);    
		return image;
	}
	
	public static String saveImage(byte[] b) throws IOException {
		String path = "/cache/" + rand.nextInt(999999999);
		File f = new File("." + path);
		FileOutputStream fos = new FileOutputStream(f);
		BufferedOutputStream bos = new BufferedOutputStream(fos);
		bos.write(b);
		bos.flush();
		bos.close();
		return path;
	}
	
	public ResultSet imageSearch(int ifeature, byte[] obj, int d, int bitDiff,
			int lire_maxhits, String lire_searcher, String lire_filter, 
			List<String> osServers) throws IOException {
		ResultSet rs = null;
		List<Feature> features = new ArrayList<Feature>(conf.getFeatures().size());
		
		switch (ifeature) {
		case 0: {
			ByteArrayInputStream bais = new ByteArrayInputStream(obj);
			BufferedImage bi = ImageIO.read(bais);
			String hc = new ImagePHash().getHash(bi);
			List<String> args = new ArrayList<String>();
			args.add(d + "");
			args.add(bitDiff + "");
			features.add(new Feature(FeatureType.IMAGE_PHASH_ES, args, hc));
			break;
		}
		case 1:
			List<String> args1 = new ArrayList<String>();
			args1.add(lire_maxhits + "");
			args1.add(lire_searcher);
			args1.add(lire_filter);
			features.add(new Feature(FeatureType.IMAGE_LIRE, args1));
			break;
		case 2:
			List<String> args2 = new ArrayList<String>();
			args2.add(lire_maxhits + "");
			args2.add(lire_searcher);
			args2.add(lire_filter);
			features.add(new Feature(FeatureType.IMAGE_FACES, args2));
			break;
		case -1:
		default:
			for (FeatureType feature : conf.getFeatures()) {
				switch (feature) {
				case IMAGE_LIRE: {
					List<String> args3 = new ArrayList<String>();
					args3.add(lire_maxhits + "");
					args3.add(lire_searcher);
					args3.add(lire_filter);
					features.add(new Feature(feature, args3));
					break;
				}
				case IMAGE_FACES: {
					List<String> args4 = new ArrayList<String>();
					args4.add(lire_maxhits + "");
					args4.add(lire_searcher);
					args4.add(lire_filter);
					features.add(new Feature(feature, args4));
					break;
				}
				default:
					break;
				}
			}
		}
		
		// ok, got all the features, send them to all active servers.
		try {
			rs = ca.objectSearch(features, obj, osServers);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException("Internal object search failed: " + e.getMessage());
		}
		
		return rs;
	}
	
	private void doImageMatch(String target, Request baseRequest, 
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		if (target.equalsIgnoreCase("/os/index.html")) {
			ResourceHandler rh = new ResourceHandler();
			rh.setResourceBase(".");
			rh.handle(target, baseRequest, request, response);
		} else {
			DiskFileItemFactory factory = new DiskFileItemFactory();
			ServletFileUpload upload = new ServletFileUpload(factory);
			List<FileItem> items;
			try {
				items = upload.parseRequest(request);
				// 解析request请求
				Iterator<FileItem> iter = items.iterator();
				int distance = 0;
				int bitDiff = 0;
				BufferedImage img = null;
				byte[] obj = null;
				String filePath = "INVALID";
				int feature = 0;
				int lire_maxhits = 1;
				String lire_searcher = "CEDD";
				String lire_filter = "NONE";
				List<String> osServers = null;

				while (iter.hasNext()) {
					FileItem item = (FileItem) iter.next();
					// 如果是表单域 ，就是非文件上传元素
					if (item.isFormField()) {
						// 获取name属性的值
						String name = item.getFieldName();
						// 获取value属性的值
						String value = item.getString();
						if (name.equals("distance"))
							distance = Integer.parseInt(value);
						if (name.equals("BitDiff")) 
							bitDiff = Integer.parseInt(value);
						if (name.equals("feature")) {
							feature = Integer.parseInt(value);
						}
						if (name.equals("maxhits")) {
							lire_maxhits = Integer.parseInt(value);
						}
						if (name.equals("searcher")) {
							lire_searcher = value;
						}
						if (name.equals("filter")) {
							lire_filter = value;
						}
						if (name.equals("servers")) {
							System.out.println("GOT servers: " + value);
							if (value != null) {
								String[] _t = value.split(";");
								if (_t != null) {
									for (String t : _t) {
										if (t != null && t.contains(":")) {
											if (osServers == null) {
												osServers = new ArrayList<String>();
											}
											osServers.add(t);
										}
									}
								}
							}
						}
					} else {
						// 文件域中name属性的值
						String fieldName = item.getFieldName();
						// 文件的全路径，绝对路径名加文件名
						String fileName = item.getName();
						filePath = fileName;
						try {
							obj = item.get();
							img = readImage(item.get());
						} catch (IOException e) {
							badResponse(baseRequest, response, "#FAIL:" + 
									e.getMessage());
							e.printStackTrace();
							return;
						}
						if (img == null) {
							badResponse(baseRequest, response, 
									"#FAIL:the file uploaded probably is not an image.");
							return;
						}
					}
				}

				long beginTs = System.currentTimeMillis();
				ResultSet rs = imageSearch(feature, obj, distance, bitDiff, 
						lire_maxhits, lire_searcher, lire_filter, osServers);
				long endTs = System.currentTimeMillis();
				String page = "<HTML><HEAD> <TITLE> Feature based MM Object Search </TITLE> </HEAD>"
						+ "<BODY><H1>Search Results: </H1><ul>";
				String spath = saveImage(obj);
				if (spath != null) {
					page += "<li> Original Image <br><img width=\"100\" height=\"100\" src=\"" + 
							spath + "\"></li>";
				}
				page += "</ul><H2>File '" + filePath + "' matches " + rs.getSize() + 
						" files in " + (endTs - beginTs) + " ms.</H2><UL>";
				if (rs.getSize() > 0) {
					Iterator<Result> iter2 = rs.getResults().iterator();
					while (iter2.hasNext()) {
						Result r = iter2.next();
						page += "<li> " + String.format("%.4f", r.getScore()) + " -> " 
								+ r.getValue() + "<br><img width=\"100\" height=\"100\" src=\"http://"
								+ SearcherConf.getHttpServer() + "/get?key="
								+ r.getValue() + "\"> </li>";
					}
				}
				page += "</UL></BODY> </HTML>";
				response.setContentType("text/html;charset=utf-8");
				response.setStatus(HttpServletResponse.SC_OK);
				baseRequest.setHandled(true);
				response.getWriter().write(page);
				response.getWriter().flush();
			} catch (FileUploadException e) {
				e.printStackTrace();
				badResponse(baseRequest, response, e.getMessage());
			}
		}
	}

	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {

		if (target == null) {
			// bad response
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		} else if (target.startsWith("/os/")) {
			try {
			doImageMatch(target, baseRequest, request, response);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (target.startsWith("/cache/")) {
			doReadCache(target, baseRequest, request, response);
		} else {
			badResponse(baseRequest, response, "#FAIL: invalid target=" + target);
		}
	}

	private void doReadCache(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response) 
					throws IOException {
		File f = new File("." + target);
		byte[] b = new byte[(int) f.length()];
		FileInputStream fis;
		try {
			fis = new FileInputStream(f);
			BufferedInputStream bis = new BufferedInputStream(fis);
			bis.read(b);
			bis.close();
			
			response.setContentType("image");
			response.setStatus(HttpServletResponse.SC_OK);
			baseRequest.setHandled(true);
			response.getOutputStream().write(b);
			response.getOutputStream().flush();
		} catch (FileNotFoundException e) {
			badResponse(baseRequest, response, "#FAIL: invalid cache entry=" + target);
		}
	}
}
