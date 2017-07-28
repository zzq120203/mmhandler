package iie.metastore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

public class SysInfoStat {
	
	public static class SysStatKey implements Comparable<SysStatKey> {
		String hostname;
		String dev;
		
		public SysStatKey(String hostname, String dev) {
			this.hostname = hostname;
			this.dev = dev;
		}
		
		@Override
		public int compareTo(SysStatKey other) {
			System.out.println("THIS " + hostname + ", " + dev + "VS " + other.hostname + ", " + other.dev);
			return hostname.compareTo(other.hostname) & dev.compareTo(other.dev);
		}
	}
	
	public static class MMStat {
		Double tlen = 0.0;
		Long[] ts = new Long[2];
		Double[] wbw = new Double[2];
		Double[] rbw = new Double[2];
		Double[] rlat = new Double[2];
		Long[] wbytes = new Long[2];
		Long[] rbytes = new Long[2];
		Long[] rdelay = new Long[2];
		Long[] readN = new Long[2];
		Long[] readErr = new Long[2];
		Long[] writeN = new Long[2];
		Long[] writeErr = new Long[2];
		int nr = 0;
		boolean isUpdated = false;
		
		public MMStat() {
			ts[0] = new Long(0);
			ts[1] = new Long(0);
			wbw[0] = new Double(0);
			wbw[1] = new Double(0);
			rbw[0] = new Double(0);
			rbw[1] = new Double(0);
			rlat[0] = new Double(0);
			rlat[1] = new Double(0);
			wbytes[0] = new Long(0);
			wbytes[1] = new Long(0);
			rbytes[0] = new Long(0);
			rbytes[1] = new Long(0);
			rdelay[0] = new Long(0);
			rdelay[1] = new Long(0);
			readN[0] = new Long(0);
			readN[1] = new Long(0);
			readErr[0] = new Long(0);
			readErr[1] = new Long(0);
			writeN[0] = new Long(0);
			writeN[1] = new Long(0);
			writeErr[0] = new Long(0);
			writeErr[1] = new Long(0);
		}
		
		public MMStat(boolean isUpdated) {
			this();
			this.isUpdated = isUpdated;
		}
		
		public boolean isValid() {
			return isUpdated && (ts[1] - ts[0] >=0) &&
					(wbw[1] >= 0) &&
					(rbw[1] >= 0) &&
					(rlat[1] >= 0) &&
					(wbytes[1] - wbytes[0] >= 0) &&
					(rbytes[1] - rbytes[0] >= 0) &&
					(rdelay[1] - rdelay[0] >= 0) &&
					(readN[1] - readN[0] >= 0) &&
					(readErr[1] - readErr[0] >= 0) &&
					(writeN[1] - writeN[0] >= 0) &&
					(writeErr[1] - writeErr[0] >= 0);
		}
		
		public String toString() {
			String r = "";
			
			r += tlen + "," +
					(ts[1] - ts[0]) + "," +
					(wbw[1]) + "," +
					(rbw[1]) + "," +
					(rlat[1]) + "," +
					(wbytes[1] - wbytes[0]) + "," +
					(rbytes[1] - rbytes[0]) + "," +
					(rdelay[1] - rdelay[0]) + "," +
					(readN[1] - readN[0]) + "," +
					(readErr[1] - readErr[0]) + "," +
					(writeN[1] - writeN[0]) + "," +
					(writeErr[1] - writeErr[0]) + "," +
					nr;
			isUpdated = false;
			return r;
		}
		
		public void Add(MMStat other) {
			isUpdated = true;
			synchronized (this) {
				if (tlen == 0.0) {
					tlen = (double)(other.ts[1] - other.ts[0]);
					ts[0] = other.ts[0];
					ts[1] = other.ts[1];
				} else {
					tlen = (double) Math.min(ts[1] - ts[0], other.ts[1] - other.ts[0]);
					ts[0] = (ts[0] + other.ts[0]) / 2;
					ts[1] = (ts[1] + other.ts[1]) / 2;
				}
				wbw[0] += other.wbw[0];
				wbw[1] += other.wbw[1];
				rbw[0] += other.rbw[0];
				rbw[1] += other.rbw[1];
				rlat[0] += other.rlat[0];
				rlat[0] /= 2;
				rlat[1] += other.rlat[1];
				rlat[1] /= 2;
				wbytes[0] += other.wbytes[0];
				wbytes[1] += other.wbytes[1];
				rbytes[0] += other.rbytes[0];
				rbytes[1] += other.rbytes[1];
				rdelay[0] += other.rdelay[0];
				rdelay[1] += other.rdelay[1];
				readN[0] += other.readN[0];
				readN[1] += other.readN[1];
				readErr[0] += other.readErr[0];
				readErr[1] += other.readErr[1];
				writeN[0] += other.writeN[0];
				writeN[1] += other.writeN[1];
				writeErr[0] += other.writeErr[0];
				writeErr[1] += other.writeErr[1];
				nr += 1;
			}
		}
		
		public void Update(Long ts, Double wbw, Double rbw, Double rlat, Long wbytes, 
				Long rbytes, Long rdelay, Long readN, Long readErr, Long writeN, Long writeErr) {
			isUpdated = true;
			synchronized (this) {
				this.ts[0] = this.ts[1];
				this.wbw[0] = this.wbw[1];
				this.rbw[0] = this.rbw[1];
				this.rlat[0] = this.rlat[1];
				this.wbytes[0] = this.wbytes[1];
				this.rbytes[0] = this.rbytes[1];
				this.rdelay[0] = this.rdelay[1];
				this.readN[0] = this.readN[1];
				this.readErr[0] = this.readErr[1];
				this.writeN[0] = this.writeN[1];
				this.writeErr[0] = this.writeErr[1];
				
				this.ts[1] = ts;
				this.wbw[1] = wbw;
				this.rbw[1] = rbw;
				this.rlat[1] = rlat;
				this.wbytes[1] = wbytes;
				this.rbytes[1] = rbytes;
				this.rdelay[1] = rdelay;
				this.readN[1] = readN;
				this.readErr[1] = readErr;
				this.writeN[1] = writeN;
				this.writeErr[1] = writeErr;
			}
		}
	}
	
	public static class NetStat {
		Double tlen = 0.0;
		Long[] ts = new Long[2];
		Long[] rx_bytes = new Long[2];
		Long[] rx_packets = new Long[2];
		Long[] rx_errs = new Long[2];
		Long[] rx_drop = new Long[2];
		Long[] rx_fifo = new Long[2];
		Long[] rx_frame = new Long[2];
		Long[] rx_compressed = new Long[2];
		Long[] rx_multicast = new Long[2];
		Long[] tx_bytes = new Long[2];
		Long[] tx_packets = new Long[2];
		Long[] tx_errs = new Long[2];
		Long[] tx_drop = new Long[2];
		Long[] tx_fifo = new Long[2];
		Long[] tx_frame = new Long[2];
		Long[] tx_compressed = new Long[2];
		Long[] tx_multicast = new Long[2];
		
		public NetStat() {
			ts[0] = new Long(0);
			ts[1] = new Long(0);
			rx_bytes[0] = new Long(0);
			rx_bytes[1] = new Long(0);
			rx_packets[0] = new Long(0);
			rx_packets[1] = new Long(0);
			rx_errs[0] = new Long(0);
			rx_errs[1] = new Long(0);
			rx_drop[0] = new Long(0);
			rx_drop[1] = new Long(0);
			rx_fifo[0] = new Long(0);
			rx_fifo[1] = new Long(0);
			rx_frame[0] = new Long(0);
			rx_frame[1] = new Long(0);
			rx_compressed[0] = new Long(0);
			rx_compressed[1] = new Long(0);
			rx_multicast[0] = new Long(0);
			rx_multicast[1] = new Long(0);
			tx_bytes[0] = new Long(0);
			tx_bytes[1] = new Long(0);
			tx_packets[0] = new Long(0);
			tx_packets[1] = new Long(0);
			tx_errs[0] = new Long(0);
			tx_errs[1] = new Long(0);
			tx_drop[0] = new Long(0);
			tx_drop[1] = new Long(0);
			tx_fifo[0] = new Long(0);
			tx_fifo[1] = new Long(0);
			tx_frame[0] = new Long(0);
			tx_frame[1] = new Long(0);
			tx_compressed[0] = new Long(0);
			tx_compressed[1] = new Long(0);
			tx_multicast[0] = new Long(0);
			tx_multicast[1] = new Long(0);
		}
		
		public String toString() {
			String r = "";
			
			r += tlen + "," +
					(ts[1] - ts[0]) + "," +
					(rx_bytes[1] - rx_bytes[0]) + "," +
					(rx_packets[1] - rx_packets[0]) + "," +
					(rx_errs[1] - rx_errs[0]) + "," +
					(rx_drop[1] - rx_drop[0]) + "," +
					(rx_fifo[1] - rx_fifo[0]) + "," +
					(rx_frame[1] - rx_frame[0]) + "," +
					(rx_compressed[1] - rx_compressed[0]) + "," +
					(rx_multicast[1] - rx_multicast[0]) + "," +
					(tx_bytes[1] - tx_bytes[0]) + "," +
					(tx_packets[1] - tx_packets[0]) + "," +
					(tx_errs[1] - tx_errs[0]) + "," +
					(tx_drop[1] - tx_drop[0]) + "," +
					(tx_fifo[1] - tx_fifo[0]) + "," +
					(tx_frame[1] - tx_frame[0]) + "," +
					(tx_compressed[1] - tx_compressed[0]) + "," +
					(tx_multicast[1] - tx_multicast[0]);
			return r;
		}
		
		public void Add(NetStat other) {
			synchronized (this) {
				if (tlen == 0.0) {
					tlen = (double)(other.ts[1] - other.ts[0]);
					ts[0] = other.ts[0];
					ts[1] = other.ts[1];
				} else {
					tlen = (double) Math.min(ts[1] - ts[0], other.ts[1] - other.ts[0]);
					ts[0] = (ts[0] + other.ts[0]) / 2;
					ts[1] = (ts[1] + other.ts[1]) / 2;
				}
				rx_bytes[0] += other.rx_bytes[0];
				rx_bytes[1] += other.rx_bytes[1];
				rx_packets[0] += other.rx_packets[0];
				rx_packets[1] += other.rx_packets[1];
				rx_errs[0] += other.rx_errs[0];
				rx_errs[1] += other.rx_errs[1];
				rx_drop[0] += other.rx_drop[0];
				rx_drop[1] += other.rx_drop[1];
				rx_fifo[0] += other.rx_fifo[0];
				rx_fifo[1] += other.rx_fifo[1];
				rx_frame[0] += other.rx_frame[0];
				rx_frame[1] += other.rx_frame[1];
				rx_compressed[0] += other.rx_compressed[0];
				rx_compressed[1] += other.rx_compressed[1];
				rx_multicast[0] += other.rx_multicast[0];
				rx_multicast[1] += other.rx_multicast[1];
				tx_bytes[0] += other.tx_bytes[0];
				tx_bytes[1] += other.tx_bytes[1];
				tx_packets[0] += other.tx_packets[0];
				tx_packets[1] += other.tx_packets[1];
				tx_errs[0] += other.tx_errs[0];
				tx_errs[1] += other.tx_errs[1];
				tx_drop[0] += other.tx_drop[0];
				tx_drop[1] += other.tx_drop[1];
				tx_fifo[0] += other.tx_fifo[0];
				tx_fifo[1] += other.tx_fifo[1];
				tx_frame[0] += other.tx_frame[0];
				tx_frame[1] += other.tx_frame[1];
				tx_compressed[0] += other.tx_compressed[0];
				tx_compressed[1] += other.tx_compressed[1];
				tx_multicast[0] += other.tx_multicast[0];
				tx_multicast[1] += other.tx_multicast[1];
			}
		}
		
		public void Update(Long ts, Long rx_bytes, Long rx_packets, Long rx_errs, Long rx_drop, Long rx_fifo, Long rx_frame, Long rx_compressed, Long rx_multicast,
					Long tx_bytes, Long tx_packets, Long tx_errs, Long tx_drop, Long tx_fifo, Long tx_frame, Long tx_compressed, Long tx_multicast) {
			synchronized (this) {
				this.ts[0] = this.ts[1];
				this.rx_bytes[0] = this.rx_bytes[1];
				this.rx_packets[0] = this.rx_packets[1];
				this.rx_errs[0] = this.rx_errs[1];
				this.rx_drop[0] = this.rx_drop[1];
				this.rx_fifo[0] = this.rx_fifo[1];
				this.rx_frame[0] = this.rx_frame[1];
				this.rx_compressed[0] = this.rx_compressed[1];
				this.rx_multicast[0] = this.rx_multicast[1];
				this.tx_bytes[0] = this.tx_bytes[1];
				this.tx_packets[0] = this.tx_packets[1];
				this.tx_errs[0] = this.tx_errs[1];
				this.tx_drop[0] = this.tx_drop[1];
				this.tx_fifo[0] = this.tx_fifo[1];
				this.tx_frame[0] = this.tx_frame[1];
				this.tx_compressed[0] = this.tx_compressed[1];
				this.tx_multicast[0] = this.tx_multicast[1];
				
				this.ts[1] = ts;
				this.rx_bytes[1] = rx_bytes;
				this.rx_packets[1] = rx_packets;
				this.rx_errs[1] = rx_errs;
				this.rx_drop[1] = rx_drop;
				this.rx_fifo[1] = rx_fifo;
				this.rx_frame[1] = rx_frame;
				this.rx_compressed[1] = rx_compressed;
				this.rx_multicast[1] = rx_multicast;
				this.tx_bytes[1] = tx_bytes;
				this.tx_packets[1] = tx_packets;
				this.tx_errs[1] = tx_errs;
				this.tx_drop[1] = tx_drop;
				this.tx_fifo[1] = tx_fifo;
				this.tx_frame[1] = tx_frame;
				this.tx_compressed[1] = tx_compressed;
				this.tx_multicast[1] = tx_multicast;
			}
		}
	}
	
	public static class CPUStat {
		Double tlen = 0.0;
		int cpunr = 0;
		Long[] ts = new Long[2];
		Long[] user = new Long[2];
		Long[] nice = new Long[2];
		Long[] system = new Long[2];
		Long[] idle = new Long[2];
		Long[] iowait = new Long[2];
		
		public CPUStat() {
			cpunr = 0;
			ts[0] = new Long(0);
			ts[1] = new Long(0);
			user[0] = new Long(0);
			user[1] = new Long(0);
			nice[0] = new Long(0);
			nice[1] = new Long(0);
			system[0] = new Long(0);
			system[1] = new Long(0);
			idle[0] = new Long(0);
			idle[1] = new Long(0);
			iowait[0] = new Long(0);
			iowait[1] = new Long(0);
		}
		
		public String toString() {
			String r = "";
			
			r += tlen + "," +
					(ts[1] - ts[0]) + "," +
					(user[1] - user[0]) + "," +
					(nice[1] - nice[0]) + "," +
					(system[1] - system[0]) + "," +
					(idle[1] - idle[0]) + "," +
					(iowait[1] - iowait[0]) + "," + 
					cpunr;
			
			return r;
		}
		
		public void Add(CPUStat other) {
			synchronized (this) {
				if (tlen == 0.0) {
					tlen = (double)(other.ts[1] - other.ts[0]);
					ts[0] = other.ts[0];
					ts[1] = other.ts[1];
				} else {
					tlen = (double) Math.min(ts[1] - ts[0], other.ts[1] - other.ts[0]);
					ts[0] = (ts[0] + other.ts[0]) / 2;
					ts[1] = (ts[1] + other.ts[1]) / 2;
				}
				user[0] += other.user[0];
				user[1] += other.user[1];
				nice[0] += other.nice[0];
				nice[1] += other.nice[1];
				system[0] += other.system[0];
				system[1] += other.system[1];
				idle[0] += other.idle[0];
				idle[1] += other.idle[1];
				iowait[0] += other.iowait[0];
				iowait[1] += other.iowait[1];
				cpunr += other.cpunr;
			}
		}
		
		public void Update(Long cts, Long user, Long nice, Long system, Long idle, 
				Long iowait, int cpunr) {
			synchronized (this) {
				this.cpunr = cpunr;
				
				ts[0] = ts[1];
				this.user[0] = this.user[1];
				this.nice[0] = this.nice[1];
				this.system[0] = this.system[1];
				this.idle[0] = this.idle[1];
				this.iowait[0] = this.iowait[1];
				
				ts[1] = cts;
				this.user[1] = user;
				this.nice[1] = nice;
				this.system[1] = system;
				this.idle[1] = idle;
				this.iowait[1] = iowait;
			}
		}
	}
	
	public static class DiskStat {
		Double tlen = 0.0;
		Long[] ts = new Long[2];
		Long[] read = new Long[2];
		Long[] rmerge = new Long[2];
		Long[] rrate = new Long[2];
		Long[] rlat = new Long[2];
		Long[] write = new Long[2];
		Long[] wmerge = new Long[2];
		Long[] wrate = new Long[2];
		Long[] wlat = new Long[2];
		Long[] pIO = new Long[2];
		Long[] alat = new Long[2];
		Long[] wtlat = new Long[2];
		
		public DiskStat() {
			ts[0] = new Long(0);
			ts[1] = new Long(0);
			read[0] = new Long(0);
			read[1] = new Long(0);
			rmerge[0] = new Long(0);
			rmerge[1] = new Long(0);
			rrate[0] = new Long(0);
			rrate[1] = new Long(0);
			rlat[0] = new Long(0);
			rlat[1] = new Long(0);
			write[0] = new Long(0);
			write[1] = new Long(0);
			wmerge[0] = new Long(0);
			wmerge[1] = new Long(0);
			wrate[0] = new Long(0);
			wrate[1] = new Long(0);
			wlat[0] = new Long(0);
			wlat[1] = new Long(0);
			pIO[0] = new Long(0);
			pIO[1] = new Long(0);
			alat[0] = new Long(0);
			alat[1] = new Long(0);
			wtlat[0] = new Long(0);
			wtlat[1] = new Long(0);
		}
		
		public String toString() {
			String r = "";
		
			r += tlen + "," + 
					(ts[1] - ts[0]) + "," +
					(read[1] - read[0]) + "," +
					(rmerge[1] - rmerge[0]) + "," +
					(rrate[1] - rrate[0]) + "," +
					(rlat[1] - rlat[0]) + "," +
					(write[1] - write[0]) + "," +
					(wmerge[1] - wmerge[0]) + "," +
					(wrate[1] - wrate[0]) + "," +
					(wlat[1] - wlat[0]) + "," +
					(pIO[1]) + "," +
					(alat[1] - alat[0]) / (pIO[1] == 0 ? 1 : pIO[1]) + "," +
					(wtlat[1] - wtlat[0])/ (pIO[1] == 0 ? 1 : pIO[1]);
			return r;
		}
		
		public void Add(DiskStat other) {
			synchronized (this) {
				if (tlen == 0.0) {
					tlen = (double)(other.ts[1] - other.ts[0]);
					ts[0] = other.ts[0];
					ts[1] = other.ts[1];
				} else {
					tlen = (double) Math.min(ts[1] - ts[0], other.ts[1] - other.ts[0]);
					ts[0] = (ts[0] + other.ts[0]) / 2;
					ts[1] = (ts[1] + other.ts[1]) / 2;
				}
				read[0] += other.read[0];
				read[1] += other.read[1];
				rmerge[0] += other.rmerge[0];
				rmerge[1] += other.rmerge[1];
				rrate[0] += other.rrate[0];
				rrate[1] += other.rrate[1];
				rlat[0] += other.rlat[0];
				rlat[1] += other.rlat[1];
				write[0] += other.write[0];
				write[1] += other.write[1];
				wmerge[0] += other.wmerge[0];
				wmerge[1] += other.wmerge[1];
				wrate[0] += other.wrate[0];
				wrate[1] += other.wrate[1];
				wlat[0] += other.wlat[0];
				wlat[1] += other.wlat[1];
				pIO[0] += other.pIO[0];
				pIO[1] += other.pIO[1];
				alat[0] += other.alat[0];
				alat[1] += other.alat[1];
				wtlat[0] += other.wtlat[0];
				wtlat[1] += other.wtlat[1];
			}
		}
		
		public void Update(Long ts, Long read, Long rmerge, Long rrate, Long rlat, 
				Long write, Long wmerge, Long wrate, Long wlat,
				Long pIO, Long alat, Long wtlat) {
			synchronized (this) {
				this.ts[0] = this.ts[1];
				this.read[0] = this.read[1];
				this.rmerge[0] = this.rmerge[1];
				this.rrate[0] = this.rrate[1];
				this.rlat[0] = this.rlat[1];
				this.write[0] = this.write[1];
				this.wmerge[0] = this.wmerge[1];
				this.wrate[0] = this.wrate[1];
				this.wlat[0] = this.wlat[1];
				this.pIO[0] = this.pIO[1];
				this.alat[0] = this.alat[1];
				this.wtlat[0] = this.wtlat[1];

				this.ts[1] = ts;
				this.read[1] = read;
				this.rmerge[1] = rmerge;
				this.rrate[1] = rrate;
				this.rlat[1] = rlat;
				this.write[1] = write;
				this.wmerge[1] = wmerge;
				this.wrate[1] = wrate;
				this.wlat[1] = wlat;
				this.pIO[1] = pIO;
				this.alat[1] = alat;
				this.wtlat[1] = wtlat;
			}
		}
	}
	
	public static class Server implements Runnable {
		private boolean do_report = false;
		public int bsize = 65536;
		public int listenPort = 19888;
		public int interval = 5;
		public DatagramSocket server;
		public static Map<String, SysStatKey> dskMap = new ConcurrentHashMap<String, SysStatKey>();
		public static Map<SysStatKey, DiskStat> dsMap = new ConcurrentHashMap<SysStatKey, DiskStat>();
		public static Map<String, CPUStat> cpuMap = new ConcurrentHashMap<String, CPUStat>();
		public static Map<String, SysStatKey> nskMap = new ConcurrentHashMap<String, SysStatKey>();
		public static Map<SysStatKey, NetStat> nsMap = new ConcurrentHashMap<SysStatKey, NetStat>();
		public static Map<String, SysStatKey> mmkMap = new ConcurrentHashMap<String, SysStatKey>();
		public static Map<SysStatKey, MMStat> mmMap = new ConcurrentHashMap<SysStatKey, MMStat>();
		public ServerReportTask spt = new ServerReportTask();
		public Timer timer = new Timer("ServerReportTask");
		public String prefix = "sysinfo"; 
		
		public Server(int port, int interval, String prefix) throws SocketException {
			if (port > 0)
				listenPort = port;
			this.interval = interval;
			server = new DatagramSocket(listenPort);
			timer.schedule(spt, 3000, interval * 1000);
		}
		
		public class ServerReportTask extends TimerTask {
			public Map<String, DiskStat> nodeMap = new ConcurrentHashMap<String, DiskStat>();
			public Map<String, NetStat> nnMap = new ConcurrentHashMap<String, NetStat>();
			public Map<String, MMStat> mMap = new ConcurrentHashMap<String, MMStat>();
			
			@Override
			public void run() {
				// handle SysInfoStat
				if (do_report) {
					Date d = new Date(System.currentTimeMillis());
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					File reportFile = new File("log/" + prefix + "-" + sdf.format(d));
					if (reportFile.getParentFile() != null)
						if (!reportFile.getParentFile().exists() && !reportFile.getParentFile().mkdirs()) {
							return;
						}
					
					StringBuffer sb = new StringBuffer(2048);
					
					// handle disk info
					for (Map.Entry<SysStatKey, DiskStat> e : dsMap.entrySet()) {
						DiskStat ds = nodeMap.get(e.getKey().hostname);
						if (ds == null) {
							ds = new DiskStat();
							nodeMap.put(e.getKey().hostname, ds);
						}
						synchronized (e.getValue()) {
							ds.Add(e.getValue());
						}
						if (e.getValue().ts[1] - e.getValue().ts[0] < 1024)
							sb.append("RPT_DEV -> " + e.getKey().hostname + "," + e.getKey().dev + "," + 
									(System.currentTimeMillis() / 1000) + "," + e.getValue() + "\n");
					}
					DiskStat alldevs = new DiskStat();
					boolean isAll = false;
					for (Map.Entry<String, DiskStat> e : nodeMap.entrySet()) {
						if (e.getValue().tlen > 0 && e.getValue().tlen < 1024) {
							alldevs.Add(e.getValue());
							isAll = true;
							sb.append("RPT_NODE -> " + e.getKey() + "," + 
									(System.currentTimeMillis() / 1000) + "," + e.getValue() + "\n");
						}
					}
					if (isAll) {
						sb.append("RPT_NODE -> ALL_DEVS," + 
								(System.currentTimeMillis() / 1000) + "," + alldevs + "\n");
					}
					nodeMap.clear();
					
					CPUStat allcpus = new CPUStat();
					isAll = false;
					for (Map.Entry<String, CPUStat> e : cpuMap.entrySet()) {
						if (e.getValue().ts[1] - e.getValue().ts[0] < 1024) {
							allcpus.Add(e.getValue());
							isAll = true;
							sb.append("RPT_CPU -> " + e.getKey() + "," +
									(System.currentTimeMillis() / 1000) + "," +
									e.getValue() + "\n");
						}
					}
					if (isAll) {
						sb.append("RPT_CPU -> ALL_CPUS," + 
								(System.currentTimeMillis() / 1000) + "," + allcpus + "\n");
					}
					
					// handle net info
					for (Map.Entry<SysStatKey, NetStat> e : nsMap.entrySet()) {
						NetStat ns = nnMap.get(e.getKey().hostname);
						if (ns == null) {
							ns = new NetStat();
							nnMap.put(e.getKey().hostname, ns);
						}
						synchronized (e.getValue()) {
							ns.Add(e.getValue());
						}
						if (e.getValue().ts[1] - e.getValue().ts[0] < 1024)
							sb.append("RPT_NDV -> " + e.getKey().hostname + ","	+ e.getKey().dev + "," +
									(System.currentTimeMillis() / 1000) + "," +
									e.getValue() + "\n");
					}
					NetStat allnets = new NetStat();
					isAll = false;
					for (Map.Entry<String, NetStat> e : nnMap.entrySet()) {
						if (e.getValue().tlen > 0 && e.getValue().tlen < 1024) {
							allnets.Add(e.getValue());
							isAll = true;
							sb.append("RPT_NET -> " + e.getKey() + "," +
									(System.currentTimeMillis() / 1000) + "," +
									e.getValue() + "\n");
						}
					}
					if (isAll) {
						sb.append("RPT_NET -> ALL_NETS," + 
								(System.currentTimeMillis() / 1000) + "," + allnets + "\n");
					}
					nnMap.clear();
					
					// handle MM info
					for (Map.Entry<SysStatKey, MMStat> e : mmMap.entrySet()) {
						MMStat mms = mMap.get(e.getKey().hostname);
						if (mms == null) {
							mms = new MMStat();
							mMap.put(e.getKey().hostname, mms);
						}
						synchronized (e.getValue()) {
							if (e.getValue().isValid())
								mms.Add(e.getValue());
						}
						if (e.getValue().isValid() && e.getValue().ts[1] - e.getValue().ts[0] < 1024)
							sb.append("RPT_MMX -> " + e.getKey().hostname + "," + e.getKey().dev + "," +
									(System.currentTimeMillis() / 1000) + "," +
									e.getValue() + "\n");
					}
					MMStat allmms = new MMStat(true);
					isAll = false;
					for (Map.Entry<String, MMStat> e : mMap.entrySet()) {
						if (e.getValue().tlen > 0 && e.getValue().tlen < 1024) {
							allmms.Add(e.getValue());
							isAll = true;
							if (e.getValue().isValid())
								sb.append("RPT_MMA -> " + e.getKey() + "," +
										(System.currentTimeMillis() / 1000) + "," +
										e.getValue() + "\n");
						}
					}
					if (isAll && allmms.isValid()) {
						sb.append("RPT_MMA -> ALL_MMS," +
								(System.currentTimeMillis() / 1000) + "," + allmms + "\n");
					}
					mMap.clear();
					
					// ok, write to file
					try {
						if (!reportFile.exists()) {
							reportFile.createNewFile();
						}
						FileWriter fw = new FileWriter(reportFile.getAbsoluteFile(), true);
						BufferedWriter bw = new BufferedWriter(fw);
						bw.write(sb.toString());
						bw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					do_report = false;
				}
			}
		}

		@Override
		public void run() {
			while (true) {
				byte[] recvBuf = new byte[bsize];
				DatagramPacket recvPacket = new DatagramPacket(recvBuf, recvBuf.length);
				try {
					server.receive(recvPacket);
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}
				String recvStr = new String(recvPacket.getData(), 0, recvPacket.getLength());
				//System.out.println("RECV: " + recvStr);
				do_report = true;
				
				if (recvStr.length() <= 0)
					continue;
				
				String[] lines = recvStr.split("\n");
				
				for (String line : lines) {
					String[] ds = line.split(",");
					if (ds.length == 13) {
						// MM Stat
						SysStatKey mmk = mmkMap.get(ds[0] + ":" + ds[1]); // serverName:port
						if (mmk == null) {
							mmk = new SysStatKey(ds[0], ds[1]);
							mmkMap.put(ds[0] + ":" + ds[1], mmk);
						}
						MMStat mms = mmMap.get(mmk);
						if (mms == null) {
							mms = new MMStat();
							mmMap.put(mmk, mms);
							System.out.println("Alloc MMSrv " + mmk.hostname + ":" + mmk.dev);
						}
						if (Long.parseLong(ds[2]) > mms.ts[1]) {
							mms.Update(Long.parseLong(ds[2]), 
									Double.parseDouble(ds[3]),
									Double.parseDouble(ds[4]),
									Double.parseDouble(ds[5]),
									Long.parseLong(ds[6]),
									Long.parseLong(ds[7]),
									Long.parseLong(ds[8]),
									Long.parseLong(ds[9]),
									Long.parseLong(ds[10]),
									Long.parseLong(ds[11]),
									Long.parseLong(ds[12]));
						}
					} else if (ds.length == 14) {
						// DiskStat
						SysStatKey dsk = dskMap.get(ds[0] + ":" + ds[1]);
						if (dsk == null) {
							dsk = new SysStatKey(ds[0], ds[1]);
							dskMap.put(ds[0] + ":" + ds[1], dsk);
						}
						DiskStat cds = dsMap.get(dsk);
						if (cds == null) {
							cds = new DiskStat();
							dsMap.put(dsk, cds);
							System.out.println("Alloc DISK " + dsk.hostname + ":" + dsk.dev);
						}
						if (Long.parseLong(ds[2]) > cds.ts[1]) {
							cds.Update(Long.parseLong(ds[2]), 
									Long.parseLong(ds[3]), 
									Long.parseLong(ds[4]), 
									Long.parseLong(ds[5]), 
									Long.parseLong(ds[6]), 
									Long.parseLong(ds[7]), 
									Long.parseLong(ds[8]), 
									Long.parseLong(ds[9]), 
									Long.parseLong(ds[10]), 
									Long.parseLong(ds[11]), 
									Long.parseLong(ds[12]), 
									Long.parseLong(ds[13]));
							//System.out.println("UPDATE -> " + cds);
						}
					} else if (ds.length == 11) {
						// CPUStat
						CPUStat cs = cpuMap.get(ds[0]);
						if (cs == null) {
							cs = new CPUStat();
							cpuMap.put(ds[0], cs);
							System.out.println("Alloc CPU " + ds[0]);
						}
						if (Long.parseLong(ds[1]) > cs.ts[1]) {
							cs.Update(Long.parseLong(ds[1]), 
									Long.parseLong(ds[3]),
									Long.parseLong(ds[4]),
									Long.parseLong(ds[5]),
									Long.parseLong(ds[6]),
									Long.parseLong(ds[7]),
									Integer.parseInt(ds[10]));
						}
					} else if (ds.length == 19) {
						// NetStat
						SysStatKey ssk = nskMap.get(ds[0] + ":" + ds[2]);
						if (ssk == null) {
							ssk = new SysStatKey(ds[0], ds[2]);
							nskMap.put(ds[0] + ":" + ds[2], ssk);
						}
						NetStat ns = nsMap.get(ssk);
						if (ns == null) {
							ns = new NetStat();
							nsMap.put(ssk, ns);
							System.out.println("Alloc NET " + ssk.hostname + ":" + ssk.dev);
						}
						if (Long.parseLong(ds[1]) > ns.ts[1]) {
							ns.Update(Long.parseLong(ds[1]), 
									Long.parseLong(ds[3]),
									Long.parseLong(ds[4]), 
									Long.parseLong(ds[5]), 
									Long.parseLong(ds[6]), 
									Long.parseLong(ds[7]), 
									Long.parseLong(ds[8]), 
									Long.parseLong(ds[9]), 
									Long.parseLong(ds[10]), 
									Long.parseLong(ds[11]), 
									Long.parseLong(ds[12]), 
									Long.parseLong(ds[13]), 
									Long.parseLong(ds[14]), 
									Long.parseLong(ds[15]), 
									Long.parseLong(ds[16]), 
									Long.parseLong(ds[17]), 
									Long.parseLong(ds[18]));
						}
					}
				}
			}
		}
	}
	
	public static class Client implements Runnable {
		public String serverName;
		public int port = 19888;
		public DatagramSocket client;
		public int interval = 5;
		
		public Client(String serverName, int port, int interval) throws SocketException {
			if (port > 0)
				this.port = port;
			this.serverName = serverName;
			this.interval = interval;
			client = new DatagramSocket();
		}

		@Override
		public void run() {
			while (true) {
				String sendStr = "";
				// read proc file system
				File diskstats = new File("/proc/diskstats");
				try {
					Thread.sleep(interval * 1000);
					FileReader fr = new FileReader(diskstats.getAbsoluteFile());
					BufferedReader br = new BufferedReader(fr);
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] s = line.split(" +");
						if (s.length == 15) {
							if (s[3].matches("sd[a-z]")) {
								sendStr += InetAddress.getLocalHost().getHostName() + "," +
										s[3] + "," +
										System.currentTimeMillis() / 1000 + "," +
										s[4] + "," + 
										s[5] + "," + s[6] + "," + s[7] + "," + 
										s[8] + "," + s[9] + "," + s[10] + "," + 
										s[11] + "," + s[12] + "," + s[13] + "," +
										s[14] + "\n";
							}
						}
					}
					br.close();
					fr.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				File sysstats = new File("/proc/stat");
				int cpunr = 0;
				try {
					FileReader fr = new FileReader(sysstats.getAbsoluteFile());
					BufferedReader br = new BufferedReader(fr);
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] s = line.split(" +");
						if (s[0].matches("cpu[0-9]+")) {
							cpunr++;
						}
					}
					br.close();
					fr.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					FileReader fr = new FileReader(sysstats.getAbsoluteFile());
					BufferedReader br = new BufferedReader(fr);
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] s = line.split(" +");
						if (s[0].equalsIgnoreCase("cpu")) {
							sendStr += InetAddress.getLocalHost().getHostName() + "," +
									System.currentTimeMillis() / 1000 + "," +
									s[0] + "," +
									s[1] + "," + 
									s[2] + "," + 
									s[3] + "," + 
									s[4] + "," + 
									s[5] + "," + 
									s[6] + "," + 
									s[7] + "," + 
									cpunr + "\n"; 
						}
					}
					br.close();
					fr.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				File netstats = new File("/proc/net/dev");
				try {
					FileReader fr = new FileReader(netstats.getAbsoluteFile());
					BufferedReader br = new BufferedReader(fr);
					String line = null;
					while ((line = br.readLine()) != null) {
						String[] s0 = line.split(":");
						//if (s0[0].matches(" *wlan[0-9]+")) {
						if (s0[0].matches(" *eth[0-9]+")) {
							String[] s = s0[1].trim().split(" +");
							sendStr += InetAddress.getLocalHost().getHostName() + "," +
									System.currentTimeMillis() / 1000 + "," +
									s0[0].trim() + "," + 
									s[0] + "," +
									s[1] + "," + 
									s[2] + "," + 
									s[3] + "," + 
									s[4] + "," + 
									s[5] + "," + 
									s[6] + "," + 
									s[7] + "," + 
									s[8] + "," + 
									s[9] + "," + 
									s[10] + "," + 
									s[11] + "," + 
									s[12] + "," + 
									s[13] + "," + 
									s[14] + "," + 
									s[15] + "\n"; 
						}
					}
					br.close();
					fr.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.format("Got: {%s}\n", sendStr);
				
				if (sendStr.length() > 0) {
					byte[] sendBuf = sendStr.getBytes();
					DatagramPacket sendPacket;
					
					try {
						sendPacket = new DatagramPacket(sendBuf, sendBuf.length, 
								InetAddress.getByName(serverName), port);
						client.send(sendPacket);
					} catch (UnknownHostException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}

	public static void main(String[] args) {
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    String serverName = "localhost";
	    String prefix = "sysinfo";
	    int mode = 0, port = -1, interval = 5;
	    
		// parse the args
	    for (int i = 0; i < args.length; i++) {
	    	System.out.println("Args " + i + ", " + args[i]);
	        switch (args[i].charAt(0)) {
	        case '-':
	            if (args[i].length() < 2)
	                throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	            if (args[i].charAt(1) == '-') {
	                if (args[i].length() < 3)
	                    throw new IllegalArgumentException("Not a valid argument: "+args[i]);
	                doubleOptsList.add(args[i].substring(2, args[i].length()));
	            } else {
	                if (args.length-1 > i)
	                    if (args[i + 1].charAt(0) == '-') {
	                    	optsList.add(new Option(args[i], null));
	                    } else {
	                    	optsList.add(new SysInfoStat.Option(args[i], args[i+1]));
	                    	i++;
	                    }
	                else {
	                	optsList.add(new Option(args[i], null));
	                }
	            }
	            break;
	        default:
	            // arg
	            argsList.add(args[i]);
	            break;
	        }
	    }
	    
	    for (Option o : optsList) {
	    	if (o.flag.equals("-h")) {
	    		// print help message
	    		System.out.println("-h  : print this help.");
	    		System.out.println("-s  : server mode.");
	    		System.out.println("-c  : client mode.");
	    		System.out.println("-r  : server name.");
	    		System.out.println("-p  : server port.");
	    		System.out.println("-i  : set dump interval.");
	    		System.out.println("-f  : set log file prefix.");
	    		
	    		System.exit(0);
	    	}
	    	if (o.flag.equals("-s")) {
	    		// into server mode
	    		mode = 1;
	    	}
	    	if (o.flag.equals("-c")) {
	    		// into client mode
	    		mode = 0;
	    	}
	    	if (o.flag.equals("-r")) {
	    		// set serverName
	    		serverName = o.opt;
	    	}
	    	if (o.flag.equals("-p")) {
	    		// set serverPort
	    		port = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-i")) {
	    		// set interval
	    		interval = Integer.parseInt(o.opt);
	    	}
	    	if (o.flag.equals("-f")) {
	    		// set log file prefix
	    		prefix = o.opt;
	    	}
	    }
	    
	    try {
		    if (mode == 0) {
		    	// client mode
				Client cli = new Client(serverName, port, interval);
				cli.run();
		    } else {
		    	// server mode
		    	Server srv = new Server(port, interval, prefix);
		    	srv.run();
		    }
	    } catch (SocketException e) {
	    	e.printStackTrace();
	    }
	}
}
