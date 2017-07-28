package iie.metastore;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.MMapDirectory;

import devmap.DevMap;

public class LuceneStat {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		IndexReader reader;
		DevMap dm = new DevMap();
		String path;

		if (args.length < 2) 
			return;
		try {
			path = dm.getPath(args[0], args[1]);
			File dir = new File(path);
			long size = 0;
			
			reader = DirectoryReader.open(MMapDirectory.open(dir));
			if (dir.isDirectory()) {
				File[] files = dir.listFiles();
				if (files != null) {
					for (File f : files) {
						size += f.length();
					}
				}
			}
			System.out.println("$(" + reader.maxDoc() + "," + size + ")");
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
