package iie.mm.datasync;

public class JNIConvert {

	static {
		System.loadLibrary("silk");
	}
	
	public native byte[] silk2pcm(byte[] inSilk);
	
}
