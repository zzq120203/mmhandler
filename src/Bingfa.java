
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import iie.mm.client.ClientAPI;

public class Bingfa implements Runnable{
	
	private ClientAPI ca;
	
	public Bingfa(ClientAPI ca) {
		this.ca = ca;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		FileOutputStream fos = null;
		byte[] content;
		UUID uuid;
		
		for(int i = 0; i < 1; i ++ ){
			content = new byte[100 * 1024];
            Random r = new Random();
            r.nextBytes(content);
			uuid = UUID.randomUUID();
			File f = new File(uuid.toString());
			if(!f.exists()){
				try {
					f.createNewFile();
					fos = new FileOutputStream(f);
					fos.write(content);
					String info = ca.uploadFile(f.getPath(), "{\"type\":\"test\",\"g_id\":\"123\"}");
					System.out.println("sasdasdas");
					
					f.delete();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} finally {
					try {
						fos.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
	
}
