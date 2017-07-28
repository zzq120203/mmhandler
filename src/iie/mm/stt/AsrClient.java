package iie.mm.stt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * ASR webAPI 示例程序
 * 
 * @author unisound
 * @date 2015年1月27日
 */
public class AsrClient {
	
	private String mWebApiUrl;

	public AsrClient(String webApiUrl) {
		this.mWebApiUrl = webApiUrl;
	}

	public String parseAudio(byte[] buffer) {
		
		// 云知声开放平台网站上申请应用后获得的 appKey
		// 云知声开放平台网站上开发者的用户名
		String userId = "YZS14975990233579597";
		// 标记请求来源的标识，如用户所设备序列号 (SN)，IMEI，MAC地址等
		String deviceId = "IMEI1234567890";

		HttpURLConnection conn = null;
		boolean statusOK = false;
		int count = 1;
		do {	
		try {
			URL url = new URL(this.mWebApiUrl + "?appkey=" + Config.appKey + "&userid=" + userId + "&id=" + deviceId);
			conn = (HttpURLConnection) url.openConnection();
			// 上传的语音数据流格式
			conn.setRequestProperty("Content-Type", "audio/x-wav;codec=pcm;bit=16;rate=8000");
			// 识别结果返回的格式
			conn.setRequestProperty("Accept", "text/plain");
			// 语音数据流记录的语种及识别结果的语种
			conn.setRequestProperty("Accept-Language", "zh_CN");
			// 编码格式
			conn.setRequestProperty("Accept-Charset", "utf-8");
			// 指定ASR识别的引擎
			conn.setRequestProperty("Accept-Topic", "general");

			conn.setRequestMethod("POST");
			conn.setDoInput(true);
			conn.setDoOutput(true);
			OutputStream out = conn.getOutputStream();
			
			// 上传语音数据
			out.write(buffer);
			out.flush();
			out.close();

			// 获取结果
			if (conn.getResponseCode() == HttpURLConnection.HTTP_OK) {
				BufferedReader resultReader = new BufferedReader(new InputStreamReader(
						conn.getInputStream(), "utf-8"));
				String line = "";
				String result = "";
				while ((line = resultReader.readLine()) != null) {
					result += line;
				}
				resultReader.close();
				return result;
			}
			else {
				System.out.println("识别失败: ResponseCode=" + conn.getResponseCode());
			}
		} catch (Exception e) {
			System.out.println(e.getMessage() + " " + count++);
			if (count >= 3)
				statusOK = true;
		} finally {
			
			if (conn != null) {
				conn.disconnect();
			}
		}
		} while (!statusOK);
		return null;
	}

    private static byte[] loadFile(File file) throws IOException {
        InputStream is = new FileInputStream(file);

        long length = file.length();
        byte[] bytes = new byte[(int) length];

        int offset = 0;
        int numRead = 0;
        while (offset < bytes.length
                && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0) {
            offset += numRead;
        }

        if (offset < bytes.length) {
            is.close();
            throw new IOException("Could not completely read file " + file.getName());
        }

        is.close();
        return bytes;
    }
	
	public static void main(String[] args) throws Exception{

		AsrClient client = new AsrClient("http://api.hivoice.cn/USCService/WebApi");
		File file = new File("C:\\Users\\zzq12\\Desktop\\get20.wav");
		String result = client.parseAudio(loadFile(file));

		if(result != null) {
			System.out.println("识别结果:\n" + result);
		}
	}
}
