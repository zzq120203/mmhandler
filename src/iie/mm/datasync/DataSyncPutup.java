package iie.mm.datasync;

import java.io.File;
import java.io.FileInputStream;

import iie.mm.dao.GlobalConfig;

public class DataSyncPutup {

	private GlobalConfig gconf;
	
	private JNIConvert jc;
	
	public DataSyncPutup(GlobalConfig gc) {
		this.gconf = gc;
		this.jc = new JNIConvert();
	}
	
	public void runThrow() {
		File dirs = new File(gconf.setPath); 
		String[] dirlist = dirs.list();
		for (int i = 0; i < dirlist.length; i++) {
			File dir = new File(gconf.setPath + "/" + dirlist[i]);
			if (dir.isDirectory()) {
				DSThread t = new DSThread(dir);
				Thread tTrhead = new Thread(t);
				tTrhead.start();
			} else {
				System.out.println( "[ERR]"+ dir.getPath() + "is not exist");
			}
		}
	}
	
	class DSThread implements Runnable {
		
		private File dir;
		
		public DSThread(File dir) {
			this.dir = dir;
		}

		@Override
		public void run() {
			FileInputStream fis;
			while (true) {
				try {
					String[] filelist = dir.list();
                    Thread.sleep(1000);
                    //System.out.println("file list len : " + filelist.length);
                    for (int i = 0; i < filelist.length; i++) {
	                    File file = new File(dir.getPath() + "/" + filelist[i]);
	                    if (file.exists() && file.length() > 0) {
	                            long start = System.currentTimeMillis();
	                            String str = gconf.dcp.uploadFile(file, file.getName());
	                            long end = System.currentTimeMillis();
	                            System.out.println("[PUT] " + file.getName() + " put time : " + (end - start));
	                            if ((end - start) > 1000){
	                                    System.out.println(file.getName() + " file`s lenth is :" + file.length());
	                            }
	                            if (str != null && str != "") {
	                                    file.delete();
	                                    System.out.println("[PUT] " + file.getName() + " put time : " + (end - start));
	                            } else
	                                    System.out.println("[ERR] file : " + file.getPath() + " put failed");
	                    }
                    }
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}
	
}
