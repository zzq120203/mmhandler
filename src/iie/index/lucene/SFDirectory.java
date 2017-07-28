package iie.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hive.metastore.api.Node;
import org.apache.hadoop.hive.metastore.api.SFile;
import org.apache.hadoop.hive.metastore.api.SFileLocation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;

import devmap.DevMap;
import devmap.DevMap.DevStat;

public class SFDirectory extends Directory {
	
	public enum DirType {
		SIMPLE, NIO, MMAP, AUTO,
	}
	
	private SFile file;
	private DevMap dm;
	private DevStat ds;
	private List<SFileLocation> locations = new ArrayList<SFileLocation>();
	private SFileLocation floc;
	
	// for lucene internal use
	private File path;
	private FSDirectory ifsd;
	
	public SFDirectory(SFile file, Node thisNode, DirType dtype, LockFactory lf) throws IOException {
		if (file == null || thisNode == null || dtype == null) {
			throw new IOException("Null Node pointer provided!");
		}
		this.file = file;
		init(thisNode, dtype);
		switch (dtype) {
		default:
		case AUTO:
			ifsd = FSDirectory.open(path);
			break;
		case SIMPLE:
			ifsd = new SimpleFSDirectory(path, lf);
			break;
		case NIO:
			ifsd = new NIOFSDirectory(path, lf);
			break;
		case MMAP:
			ifsd = new MMapDirectory(path, lf);
			break;
		}
	}
	
	private void init(Node thisNode, DirType dtype) throws IOException {
		dm = new DevMap();
		
		// Step 1: filter file locations by current Node
		for (SFileLocation fl : file.getLocations()) {
			if (fl.getNode_name().equalsIgnoreCase(thisNode.getNode_name())) {
				// keep this location
				locations.add(fl);
			}
		}
		
		// Step 2: Determine the file path now, we select randomize if there are many locations.
		if (locations.size() == 1) {
			// only one valid file location
			floc = locations.get(0);
		} else if (locations.size() > 1) {
			// more than 1 local file locations?
			Random r = new Random();
			floc = locations.get(r.nextInt(locations.size()));
		} else if (locations.size() == 0) {
			// no valid file location
			throw new IOException("Invalid SFile object, no valid locaitons filtered by Node '" + 
					thisNode.getNode_name() + "'!");
		}
		ds = dm.findDev(floc.getDevid());
		locations.clear();
		if (ds == null) {
			throw new IOException("Invalid Devid '" + floc.getDevid() + "', not found in this node.");
		}
		
		// Step 3: construct a full lucene file path
		File mp = new File(ds.mount_point);
		path = new File(mp, floc.getLocation());
	}
	
	public String getFullPath() {
		return ifsd.getDirectory().getPath();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		ifsd.close();
		isOpen = false;
		ifsd = null;
	}

	@Override
	public IndexOutput createOutput(String name, IOContext context)
			throws IOException {
		return ifsd.createOutput(name, context);
	}

	@Override
	public void deleteFile(String name) throws IOException {
		ifsd.deleteFile(name);		
	}

	@Override
	public boolean fileExists(String name) throws IOException {
		return ifsd.fileExists(name);
	}

	@Override
	public long fileLength(String name) throws IOException {
		return ifsd.fileLength(name);
	}

	@Override
	public String[] listAll() throws IOException {
		return ifsd.listAll();
	}

	@Override
	public IndexInput openInput(String name, IOContext context) throws IOException {
		return ifsd.openInput(name, context);
	}

	@Override
	public void sync(Collection<String> names) throws IOException {
		ifsd.sync(names);
	}
	
	public Lock makeLock(String name) {
		return ifsd.makeLock(name);
	}
	
	public void clearLock(String name) throws IOException {
		ifsd.clearLock(name);
	}
	
	public void setLockFactory(LockFactory lockFactory) throws IOException {
		ifsd.setLockFactory(lockFactory);
	}
	
	public LockFactory getLockFactory() {
		return ifsd.getLockFactory();
	}
	
	public String getLockID() {
		return ifsd.getLockID();
	}
	
	
}
