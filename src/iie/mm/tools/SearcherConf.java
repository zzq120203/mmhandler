package iie.mm.tools;

import iie.mm.client.Feature.FeatureType;
import iie.mm.client.Feature.FeatureTypeString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class SearcherConf {
	public String serverUri = null;
	public int port = -1;
	public static Map<Long, String> hservers = new HashMap<Long, String>();
	private final static Random rand = new Random();
	
	private List<FeatureType> features = new ArrayList<FeatureType>();
	
	public SearcherConf(String uri, int port) {
		serverUri = uri;
		this.port = port;
	}
	
	public List<FeatureType> getFeatures() {
		return features;
	}

	public void addToFeatures(String features) {
		if (features.equalsIgnoreCase(FeatureTypeString.IMAGE_PHASH_ES))
			this.features.add(FeatureType.IMAGE_PHASH_ES);
		if (features.equalsIgnoreCase(FeatureTypeString.IMAGE_LIRE))
			this.features.add(FeatureType.IMAGE_LIRE);
	}
	
	public static String getFeatureTypeString(FeatureType type) {
		switch (type) {
		case IMAGE_PHASH_ES:
			return FeatureTypeString.IMAGE_PHASH_ES;
		case IMAGE_LIRE:
			return FeatureTypeString.IMAGE_LIRE;
		}
		return "none";
	}
	
	public static String getHttpServer() {
		long idx = 1 + rand.nextInt(hservers.size());
		return hservers.get(idx);
	}
}
