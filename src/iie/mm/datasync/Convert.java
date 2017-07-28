package iie.mm.datasync;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.DataLine;
import javax.sound.sampled.SourceDataLine;

public class Convert {

	private JNIConvert jc = new JNIConvert();	

	public byte[] convertAudioFiles(byte[] src) throws Exception {
		src = jc.silk2pcm(src);
		System.out.println();	
		int PCMSize = src.length;
		
		// 填入参数，比特率等等。这里用的是16位单声道 8000 hz
		WaveHeader header = new WaveHeader();
		// 长度字段 = 内容的大小（PCMSize) + 头部字段的大小(不包括前面4字节的标识符RIFF以及fileLength本身的4字节)
		header.fileLength = PCMSize + (44 - 8);
		header.FmtHdrLeth = 16;
		header.BitsPerSample = 16;
		header.Channels = 1;
		header.FormatTag = 0x0001;
		header.SamplesPerSec = 8000;
		header.BlockAlign = (short) (header.Channels * header.BitsPerSample / 8);
		header.AvgBytesPerSec = header.BlockAlign * header.SamplesPerSec;
		header.DataHdrLeth = PCMSize;

		byte[] h = header.getHeader();

		assert h.length == 44; // WAV标准，头部应该是44字节
		
		byte[] target = new byte[h.length + src.length];
		System.arraycopy(h, 0, target, 0, h.length);
		System.arraycopy(src, 0, target, h.length, src.length);
		System.out.println("Convert OK!");
		return target;
	}

	public static void main(String[] args) {
		try {
			File silkF = new File(args[0]);
			FileInputStream fis = new FileInputStream(silkF);
			byte[] buf = new byte[1024 * 4];
			int size = fis.read(buf);
			int PCMSize = 0;
			while (size != -1) {
				PCMSize += size;
				size = fis.read(buf);
			}
			fis.close();
			
			buf = new JNIConvert().silk2pcm(buf);

			FileOutputStream fos = new FileOutputStream(args[1]);
			fos.write(new Convert().convertAudioFiles(buf));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*
		System.exit(0);
		try {
			File file = new File("C:\\Users\\zzq12\\Desktop\\get");
			System.out.println(file.length());
			int offset = 0;
			int bufferSize = Integer.valueOf(String.valueOf(file.length()));
			byte[] audioData = new byte[bufferSize];
			InputStream in = new FileInputStream(file);
			in.read(audioData);

			float sampleRate = 24000;
			int sampleSizeInBits = 16;
			int channels = 1;
			boolean signed = true;
			boolean bigEndian = false;
			//  sampleRate - 每秒的样本数  
			//  sampleSizeInBits - 每个样本中的位数  
			//  channels - 声道数（单声道 1 个，立体声 2 个）  
			//  signed - 指示数据是有符号的，还是无符号的  
			//  bigEndian - 指示是否以 big-endian 字节顺序存储单个样本中的数据（false 意味着  
			//  little-endian）。  
			AudioFormat af = new AudioFormat(sampleRate, sampleSizeInBits, channels, signed, bigEndian);
			SourceDataLine.Info info = new DataLine.Info(SourceDataLine.class, af, bufferSize);
			SourceDataLine sdl = (SourceDataLine) AudioSystem.getLine(info);
			sdl.open(af);
			sdl.start();
			while (offset < audioData.length) {
				offset += sdl.write(audioData, offset, bufferSize);
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		*/
	}
}
