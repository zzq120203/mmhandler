
import iie.mm.client.ClientAPI;
import javax.sql.DataSource;

public class TestUpload {
	
	public static ClientAPI ca = new ClientAPI();
	
	public static void init() throws Exception {
		ca.init("STL://10.136.140.146:26379;10.136.140.147:26379", "TEST");
	}
	
	public static void main(String[] args) throws Exception {
		init();
//		byte[] b = ca.get("i1499961601@a7e7ec1482992656fc03b4d650f373c5");
//		byte[] b = ca.downloadFile("i1499961601@a7e7ec1482992656fc03b4d650f373c5", 0, -1);
//		System.out.println(b.length);
		System.out.println(ca.uploadFile("C:\\Users\\zzq12\\Desktop\\103.jpg", "{\"type\":\"test\",\"g_id\":\"123\"}"));
	}
}
