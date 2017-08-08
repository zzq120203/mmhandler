
import iie.mm.client.ClientAPI;
import iie.mm.tools.SimHashTools;

import java.io.File;
import java.io.FileOutputStream;

import javax.sql.DataSource;

public class TestUpload {
	
	public static ClientAPI ca = new ClientAPI();
	
	public static void init() throws Exception {
		ca.init("STL://10.144.16.56:26379;10.144.16.57:26379", "TEST");
//		ca.init("STL://10.136.140.146:26379;10.136.140.147:26379", "TEST");
	}
	
	public static void main(String[] args) throws Exception {
		init();
		
		byte[] b = ca.get("a1501657200@cf112d91336fe57fe26565b7266f4cd1");
		File file = new File("a1501657200@cf112d91336fe57fe26565b7266f4cd1.silk");
		FileOutputStream out = new FileOutputStream(file);
		out.write(b);
		out.flush();
		out.close();
		
//		byte[] b = ca.downloadFile("i1499961601@a7e7ec1482992656fc03b4d650f373c5", 0, -1);
//		System.out.println(b.length);
//		System.out.println(ca.uploadFile("C:\\Users\\zzq12\\Desktop\\b251f08437b461326f2d26294c466edf.mp4", "{\"type\":\"test\",\"g_id\":\"98989898989898\"}"));
		
//	System.out.println(SimHashTools.genSimHashCode("原文“字符类的反向选择 [^] ：如果想要搜索到有 oo 的行，但不想要 oo 前面有 g，如下”     处貌似有点问题，和我在Ubuntu 16.04测试的结果不一样"));	
		
	}
}
