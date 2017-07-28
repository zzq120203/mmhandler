package iie.mm.server;

import iie.mm.client.Feature.FeatureLIREType;
import iie.mm.client.ResultSet;
import iie.mm.client.ResultSet.Result;
import net.semanticmetadata.lire.*;
import net.semanticmetadata.lire.filter.LsaFilter;
import net.semanticmetadata.lire.filter.RerankFilter;
import net.semanticmetadata.lire.imageanalysis.*;
import net.semanticmetadata.lire.imageanalysis.joint.JointHistogram;
import net.semanticmetadata.lire.impl.VisualWordsImageSearcher;
import net.semanticmetadata.lire.utils.LuceneUtils;
import net.semanticmetadata.lire.utils.LuceneUtils.AnalyzerType;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.*;
import java.util.Map.Entry;

public class FeatureIndex {
    private static FaceDetector fd = null;

    public class LIndex {
        public static final int SIMPLE = 0;
        public static final int LIRE = 1;
        public static final int FACES = 2;

        public Directory dir = null;
        public IndexWriterConfig iwc = null;
        public IndexWriter writer = null;
        public Analyzer analyzer = null;

        public DirectoryReader reader = null;
        public Object searcher = null;
        private FeatureLIREType sType = null, fType = null;
        private int sMaxHit = 0;
        private SearchHitsFilter filter = null;

        public boolean isDirty = false;

        public LIndex(String path, int mode) throws IOException {
            dir = FSDirectory.open(new File(path));
            switch (mode) {
                case SIMPLE:
                    analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
                    iwc = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
                    iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
                    writer = new IndexWriter(dir, iwc);
                    break;
                case LIRE:
                /*analyzer = new SimpleAnalyzer(Version.LUCENE_CURRENT);
				iwc = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
				iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
				iwc.setCodec(new LireCustomCodec());
				writer = new IndexWriter(dir, iwc);*/
                    writer = LuceneUtils.createIndexWriter(dir, false, AnalyzerType.SimpleAnalyzer);
                    break;
                case FACES:
                    writer = LuceneUtils.createIndexWriter(dir, false, AnalyzerType.SimpleAnalyzer);
                    break;
            }
        }
    }

    ;

    public static List<LIndex> indexs = new ArrayList<LIndex>();

    public long reopenTo = 10 * 1000;
    public long reopenTs = System.currentTimeMillis();

    public ServerConf conf;
    public int gramSize = 4;

    public FeatureIndex(ServerConf conf) throws IOException {
        if (fd == null && conf.getFaceDetectorXML() != null) {
            fd = new FaceDetector(conf.getFaceDetectorXML());
        }
        indexs.add(new LIndex(conf.getFeatureIndexPath() + "/simple", LIndex.SIMPLE));
        indexs.add(new LIndex(conf.getFeatureIndexPath() + "/lire", LIndex.LIRE));
        indexs.add(new LIndex(conf.getFeatureIndexPath() + "/faces", LIndex.FACES));
        Timer t = new Timer();
        t.schedule(new CommitTask(), 1 * 1000, 5 * 1000);
    }

    public static class NGramAnalyzer extends Analyzer {
        private int gramSize = 4;

        public NGramAnalyzer(int gramSize) {
            this.gramSize = gramSize;
        }

        @Override
        protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
            Tokenizer source = new NGramTokenizer(reader, gramSize, gramSize);
            TokenStream filter = new LowerCaseFilter(Version.LUCENE_CURRENT, source);
            return new TokenStreamComponents(source, filter);
        }
    }

    private class CommitTask extends TimerTask {

        @Override
        public void run() {
            do_commit();
        }
    }

    private void do_commit() {
        if (System.currentTimeMillis() - reopenTs >= reopenTo) {
            for (LIndex li : indexs) {
                try {
                    if (li.isDirty) {
                        li.writer.commit();
                        li.isDirty = false;
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                reopenTs = System.currentTimeMillis();
            }
        }
    }

    /**
     * @param key   feature hash value
     * @param field feature name
     * @param value set@md5
     * @return
     */
    public boolean addObject(String key, String field, String value) {
        boolean r = false;
        IndexWriter writer = indexs.get(LIndex.SIMPLE).writer;

        if (writer != null) {
            Document doc = new Document();
            for (int i = 0; i < 16; i++) {
                doc.add(new StringField(field + "_" + i, key.substring(i, i + 4), Field.Store.YES));
            }
            doc.add(new StringField("objkey", value, Field.Store.YES));
            try {
                writer.addDocument(doc);
                r = true;
                indexs.get(LIndex.SIMPLE).isDirty = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
            //System.out.println("AddObj " + field + "=" + key + " -> " + value + " r=" + r);
        }

        return r;
    }

    public boolean addObjectLIRE(Document doc, int idx, String value) {
        boolean r = false;
        IndexWriter writer = indexs.get(idx).writer;

        if (writer != null) {
            try {
                writer.addDocument(doc);
                r = true;
                indexs.get(idx).isDirty = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
            //System.out.println("AddObj " + idx + "=" + doc.getFields() + " -> " + value + " r=" + r);
        }

        return r;
    }

    private final static Comparator<Entry<Integer, Result>> comp = new Comparator<Entry<Integer, Result>>() {
        @Override
        public int compare(Entry<Integer, Result> o1, Entry<Integer, Result> o2) {
            return (o2.getValue().getScore() - o1.getValue().getScore()) > 0 ? 1 : -1;
        }
    };

    /**
     * @param key   feature hash value
     * @param field feature name
     * @return
     * @throws IOException
     */
    public static synchronized ResultSet getObject(String key, String field, int maxEdits, int bitDiffInBlock) throws IOException {
        ResultSet rs = new ResultSet(ResultSet.ScoreMode.ADD);
        // docId -> Result
        HashMap<Integer, Result> m = new HashMap<Integer, Result>();
        IndexWriter writer = indexs.get(LIndex.SIMPLE).writer;
        DirectoryReader reader = indexs.get(LIndex.SIMPLE).reader;
        IndexSearcher searcher = (IndexSearcher) indexs.get(LIndex.SIMPLE).searcher;

        if (reader == null) {
            reader = DirectoryReader.open(indexs.get(LIndex.SIMPLE).dir);
        } else {
            DirectoryReader changed = DirectoryReader.openIfChanged(reader, writer, true);
            if (changed != null)
                reader = changed;
        }
        indexs.get(LIndex.SIMPLE).reader = reader;
        if (searcher == null || searcher.getIndexReader() != reader) {
            searcher = new IndexSearcher(reader);
            indexs.get(LIndex.SIMPLE).searcher = searcher;
        }

        long beginTs = System.currentTimeMillis();
        for (int i = 0; i < 16; i++) {
            //Query q = new TermQuery(new Term(field + "_" + i, key.substring(i, i + 4)));
            Query q = new FuzzyQuery(new Term(field + "_" + i, key.substring(i, i + 4)), bitDiffInBlock);
            ScoreDoc[] hits = searcher.search(q, searcher.getIndexReader().maxDoc()).scoreDocs;
            for (int j = 0; j < hits.length; j++) {
                System.out.println(i + "\t" + j + "\t" + hits[j].doc + "\t" + hits[j].score);
                Result nr = m.get(hits[j].doc);
                if (nr == null)
                    m.put(hits[j].doc, new Result(1, hits[j].score));
                else {
                    nr.setAuxScore(hits[j].score, ResultSet.ScoreMode.PROD);
                    rs.updateScore(nr, 1);
                    m.put(hits[j].doc, nr);
                }
            }
        }
        long endTs = System.currentTimeMillis();
        List<Entry<Integer, Result>> entries = new LinkedList<Entry<Integer, Result>>();
        entries.addAll(m.entrySet());

        Collections.sort(entries, comp);

        rs.setMode(ResultSet.ScoreMode.PROD);
        for (Entry<Integer, Result> en : entries) {
            if (en.getValue().getScore() >= 16 - maxEdits) {
                Document doc = searcher.doc(en.getKey());
                String objkey = doc.get("objkey");
                if (objkey != null) {
                    en.getValue().setValue(objkey);
                    if (bitDiffInBlock > 0) {
                        en.getValue().setScore(1 / en.getValue().getAuxScore());
                    } else
                        en.getValue().setScore(16 / en.getValue().getScore());
                    rs.addToResults(en.getValue());
                }
            }
        }

        System.out.println("Search " + field + "=" + key + " maxEdits=" + maxEdits + " -> hits " +
                rs.getSize() + " objs in " + searcher.getIndexReader().maxDoc() + " objs in " + (endTs - beginTs) + " ms.");
        return rs;
    }

    public static synchronized ResultSet getObjectLIRE(FeatureLIREType sType, FeatureLIREType fType,
                                                       BufferedImage bi, int maxHits) throws IOException {
        ResultSet rs = new ResultSet(ResultSet.ScoreMode.ADD);
        ImageSearcher searcher;
        IndexWriter writer = indexs.get(LIndex.LIRE).writer;
        DirectoryReader reader = indexs.get(LIndex.LIRE).reader;
        SearchHitsFilter filter = null;

        if (fType != indexs.get(LIndex.LIRE).fType) {
            switch (fType) {
                case JCD:
                    filter = new RerankFilter(JCD.class, DocumentBuilder.FIELD_NAME_JCD);
                    break;
                case AUTO_COLOR_CORRELOGRAM:
                    filter = new RerankFilter(AutoColorCorrelogram.class, DocumentBuilder.FIELD_NAME_AUTOCOLORCORRELOGRAM);
                    break;
                case CEDD:
                    filter = new RerankFilter(CEDD.class, DocumentBuilder.FIELD_NAME_CEDD);
                    break;
                case CEDD_HASHING:
                    filter = null;
                    break;
                case COLOR_HISTOGRAM:
                    filter = new RerankFilter(SimpleColorHistogram.class, DocumentBuilder.FIELD_NAME_COLORHISTOGRAM);
                    break;
                case COLOR_LAYOUT:
                    filter = new LsaFilter(ColorLayout.class, DocumentBuilder.FIELD_NAME_COLORLAYOUT);
                    break;
                case EDGE_HISTOGRAM:
                    filter = new LsaFilter(EdgeHistogram.class, DocumentBuilder.FIELD_NAME_EDGEHISTOGRAM);
                    break;
                case FCTH:
                    filter = new LsaFilter(FCTH.class, DocumentBuilder.FIELD_NAME_FCTH);
                    break;
                case GABOR:
                    filter = new LsaFilter(Gabor.class, DocumentBuilder.FIELD_NAME_GABOR);
                    break;
                case JOINT_HISTOGRAM:
                    filter = new LsaFilter(JointHistogram.class, DocumentBuilder.FIELD_NAME_JOINT_HISTOGRAM);
                    break;
                case JPEG_COEFF_HISTOGRAM:
                    filter = new LsaFilter(JpegCoefficientHistogram.class, DocumentBuilder.FIELD_NAME_JPEGCOEFFS);
                    break;
                case LUMINANCE_LAYOUT:
                    filter = new LsaFilter(LuminanceLayout.class, DocumentBuilder.FIELD_NAME_LUMINANCE_LAYOUT);
                    break;
                case NONE:
                    filter = null;
                    break;
                case OPPONENT_HISTOGRAM:
                    filter = new LsaFilter(OpponentHistogram.class, DocumentBuilder.FIELD_NAME_OPPONENT_HISTOGRAM);
                    break;
                case PHOG:
                    filter = new LsaFilter(PHOG.class, DocumentBuilder.FIELD_NAME_PHOG);
                    break;
                case SCALABLE_COLOR:
                    filter = new LsaFilter(ScalableColor.class, DocumentBuilder.FIELD_NAME_SCALABLECOLOR);
                    break;
                case SURF:
                case TAMURA:
                default:
                    filter = null;
                    break;
            }
            indexs.get(LIndex.LIRE).filter = filter;
            indexs.get(LIndex.LIRE).fType = fType;
        } else {
            filter = indexs.get(LIndex.LIRE).filter;
        }

        if (sType != indexs.get(LIndex.LIRE).sType ||
                maxHits >= indexs.get(LIndex.LIRE).sMaxHit) {
            switch (sType) {
                default:
                    searcher = ImageSearcherFactory.createDefaultSearcher();
                    break;
                case CEDD:
                    searcher = ImageSearcherFactory.createCEDDImageSearcher(maxHits);
                    break;
                case AUTO_COLOR_CORRELOGRAM:
                    searcher = ImageSearcherFactory.createAutoColorCorrelogramImageSearcher(maxHits);
                    break;
                case COLOR_HISTOGRAM:
                    searcher = ImageSearcherFactory.createColorHistogramImageSearcher(maxHits);
                    break;
                case COLOR_LAYOUT:
                    searcher = ImageSearcherFactory.createColorLayoutImageSearcher(maxHits);
                    break;
                case EDGE_HISTOGRAM:
                    searcher = ImageSearcherFactory.createEdgeHistogramImageSearcher(maxHits);
                    break;
                case FCTH:
                    searcher = ImageSearcherFactory.createFCTHImageSearcher(maxHits);
                    break;
                case GABOR:
                    searcher = ImageSearcherFactory.createGaborImageSearcher(maxHits);
                    break;
                case CEDD_HASHING:
                    searcher = ImageSearcherFactory.createHashingCEDDImageSearcher(maxHits);
                    break;
                case JCD:
                    searcher = ImageSearcherFactory.createJCDImageSearcher(maxHits);
                    break;
                case JOINT_HISTOGRAM:
                    searcher = ImageSearcherFactory.createJointHistogramImageSearcher(maxHits);
                    break;
                case JPEG_COEFF_HISTOGRAM:
                    searcher = ImageSearcherFactory.createJpegCoefficientHistogramImageSearcher(maxHits);
                    break;
                case LUMINANCE_LAYOUT:
                    searcher = ImageSearcherFactory.createLuminanceLayoutImageSearcher(maxHits);
                    break;
                case OPPONENT_HISTOGRAM:
                    searcher = ImageSearcherFactory.createOpponentHistogramSearcher(maxHits);
                    break;
                case PHOG:
                    searcher = ImageSearcherFactory.createPHOGImageSearcher(maxHits);
                    break;
                case SCALABLE_COLOR:
                    searcher = ImageSearcherFactory.createScalableColorImageSearcher(maxHits);
                    break;
                case TAMURA:
                    searcher = ImageSearcherFactory.createTamuraImageSearcher(maxHits);
                    break;
                case SURF:
                    searcher = new VisualWordsImageSearcher(maxHits, DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS);
                    break;
                case ZH:
                    searcher = new ZHSearcher(maxHits, 0.8f, 0.9f, 1.0f);
                    break;
            }
            indexs.get(LIndex.LIRE).sType = sType;
            indexs.get(LIndex.LIRE).sMaxHit = maxHits;
            indexs.get(LIndex.LIRE).searcher = searcher;
        } else {
            searcher = (ImageSearcher) indexs.get(LIndex.LIRE).searcher;
        }
        if (reader == null) {
            reader = DirectoryReader.open(indexs.get(LIndex.LIRE).dir);
        } else {
            DirectoryReader changed = DirectoryReader.openIfChanged(reader, writer, true);
            if (changed != null)
                reader = changed;
        }
        indexs.get(LIndex.LIRE).reader = reader;

        long beginTs = System.currentTimeMillis();
        ImageSearchHits hits = searcher.search(bi, reader);
        if (filter != null)
            hits = filter.filter(hits, hits.doc(0));
        for (int i = 0; i < hits.length() && i < maxHits; i++) {
            Result r = new Result(hits.doc(i).get("descriptorImageIdentifier"),
                    hits.score(i));
            rs.addToResults(r);
        }
        long endTs = System.currentTimeMillis();
        System.out.println("Search LIRE " + sType + " filter " + fType + " -> hits " + rs.getSize() + " objs in " + reader.maxDoc() + " objs in " + (endTs - beginTs) + " ms.");
        return rs;
    }

    public static synchronized ResultSet getObjectFaces(FeatureLIREType sType, FeatureLIREType fType,
                                                        BufferedImage bi, int maxHits) throws IOException {
        ResultSet rs = new ResultSet(ResultSet.ScoreMode.ADD);
        ImageSearcher searcher;
        IndexWriter writer = indexs.get(LIndex.FACES).writer;
        DirectoryReader reader = indexs.get(LIndex.FACES).reader;
        SearchHitsFilter filter = null;

        List<BufferedImage> faces = new ArrayList<BufferedImage>();
        if (fd != null) {
            faces = fd.detect(bi);
        }
        if (faces == null || faces.size() <= 0) {
            return rs;
        }

        if (fType != indexs.get(LIndex.FACES).fType) {
            switch (fType) {
                case JCD:
                    filter = new RerankFilter(JCD.class, DocumentBuilder.FIELD_NAME_JCD);
                    break;
                case AUTO_COLOR_CORRELOGRAM:
                    filter = new RerankFilter(AutoColorCorrelogram.class, DocumentBuilder.FIELD_NAME_AUTOCOLORCORRELOGRAM);
                    break;
                case CEDD:
                    filter = new RerankFilter(CEDD.class, DocumentBuilder.FIELD_NAME_CEDD);
                    break;
                case CEDD_HASHING:
                    filter = null;
                    break;
                case COLOR_HISTOGRAM:
                    filter = new RerankFilter(SimpleColorHistogram.class, DocumentBuilder.FIELD_NAME_COLORHISTOGRAM);
                    break;
                case COLOR_LAYOUT:
                    filter = new LsaFilter(ColorLayout.class, DocumentBuilder.FIELD_NAME_COLORLAYOUT);
                    break;
                case EDGE_HISTOGRAM:
                    filter = new LsaFilter(EdgeHistogram.class, DocumentBuilder.FIELD_NAME_EDGEHISTOGRAM);
                    break;
                case FCTH:
                    filter = new LsaFilter(FCTH.class, DocumentBuilder.FIELD_NAME_FCTH);
                    break;
                case GABOR:
                    filter = new LsaFilter(Gabor.class, DocumentBuilder.FIELD_NAME_GABOR);
                    break;
                case JOINT_HISTOGRAM:
                    filter = new LsaFilter(JointHistogram.class, DocumentBuilder.FIELD_NAME_JOINT_HISTOGRAM);
                    break;
                case JPEG_COEFF_HISTOGRAM:
                    filter = new LsaFilter(JpegCoefficientHistogram.class, DocumentBuilder.FIELD_NAME_JPEGCOEFFS);
                    break;
                case LUMINANCE_LAYOUT:
                    filter = new LsaFilter(LuminanceLayout.class, DocumentBuilder.FIELD_NAME_LUMINANCE_LAYOUT);
                    break;
                case NONE:
                    filter = null;
                    break;
                case OPPONENT_HISTOGRAM:
                    filter = new LsaFilter(OpponentHistogram.class, DocumentBuilder.FIELD_NAME_OPPONENT_HISTOGRAM);
                    break;
                case PHOG:
                    filter = new LsaFilter(PHOG.class, DocumentBuilder.FIELD_NAME_PHOG);
                    break;
                case SCALABLE_COLOR:
                    filter = new LsaFilter(ScalableColor.class, DocumentBuilder.FIELD_NAME_SCALABLECOLOR);
                    break;
                case SURF:
                case TAMURA:
                default:
                    filter = null;
                    break;
            }
            indexs.get(LIndex.FACES).filter = filter;
            indexs.get(LIndex.FACES).fType = fType;
        } else {
            filter = indexs.get(LIndex.FACES).filter;
        }

        if (sType != indexs.get(LIndex.FACES).sType ||
                maxHits >= indexs.get(LIndex.FACES).sMaxHit) {
            switch (sType) {
                default:
                    searcher = ImageSearcherFactory.createDefaultSearcher();
                    break;
                case CEDD:
                    searcher = ImageSearcherFactory.createCEDDImageSearcher(maxHits);
                    break;
                case AUTO_COLOR_CORRELOGRAM:
                    searcher = ImageSearcherFactory.createAutoColorCorrelogramImageSearcher(maxHits);
                    break;
                case COLOR_HISTOGRAM:
                    searcher = ImageSearcherFactory.createColorHistogramImageSearcher(maxHits);
                    break;
                case COLOR_LAYOUT:
                    searcher = ImageSearcherFactory.createColorLayoutImageSearcher(maxHits);
                    break;
                case EDGE_HISTOGRAM:
                    searcher = ImageSearcherFactory.createEdgeHistogramImageSearcher(maxHits);
                    break;
                case FCTH:
                    searcher = ImageSearcherFactory.createFCTHImageSearcher(maxHits);
                    break;
                case GABOR:
                    searcher = ImageSearcherFactory.createGaborImageSearcher(maxHits);
                    break;
                case CEDD_HASHING:
                    searcher = ImageSearcherFactory.createHashingCEDDImageSearcher(maxHits);
                    break;
                case JCD:
                    searcher = ImageSearcherFactory.createJCDImageSearcher(maxHits);
                    break;
                case JOINT_HISTOGRAM:
                    searcher = ImageSearcherFactory.createJointHistogramImageSearcher(maxHits);
                    break;
                case JPEG_COEFF_HISTOGRAM:
                    searcher = ImageSearcherFactory.createJpegCoefficientHistogramImageSearcher(maxHits);
                    break;
                case LUMINANCE_LAYOUT:
                    searcher = ImageSearcherFactory.createLuminanceLayoutImageSearcher(maxHits);
                    break;
                case OPPONENT_HISTOGRAM:
                    searcher = ImageSearcherFactory.createOpponentHistogramSearcher(maxHits);
                    break;
                case PHOG:
                    searcher = ImageSearcherFactory.createPHOGImageSearcher(maxHits);
                    break;
                case SCALABLE_COLOR:
                    searcher = ImageSearcherFactory.createScalableColorImageSearcher(maxHits);
                    break;
                case TAMURA:
                    searcher = ImageSearcherFactory.createTamuraImageSearcher(maxHits);
                    break;
                case SURF:
                    searcher = new VisualWordsImageSearcher(maxHits, DocumentBuilder.FIELD_NAME_SURF_VISUAL_WORDS);
                    break;
                case ZH:
                    searcher = new ZHSearcher(maxHits, 1.0f, 1.0f, 1.0f);
                    break;
            }
            indexs.get(LIndex.FACES).sType = sType;
            indexs.get(LIndex.FACES).sMaxHit = maxHits;
            indexs.get(LIndex.FACES).searcher = searcher;
        } else {
            searcher = (ImageSearcher) indexs.get(LIndex.FACES).searcher;
        }
        if (reader == null) {
            reader = DirectoryReader.open(indexs.get(LIndex.FACES).dir);
        } else {
            DirectoryReader changed = DirectoryReader.openIfChanged(reader, writer, true);
            if (changed != null)
                reader = changed;
        }
        indexs.get(LIndex.FACES).reader = reader;

        long beginTs = System.currentTimeMillis();
        for (BufferedImage face : faces) {
            ImageSearchHits hits = searcher.search(face, reader);
            if (filter != null)
                hits = filter.filter(hits, hits.doc(0));
            for (int i = 0; i < hits.length() && i < maxHits; i++) {
                Result r = new Result(hits.doc(i).get("descriptorImageIdentifier"),
                        hits.score(i));
                rs.addToResults(r);
            }
        }
        long endTs = System.currentTimeMillis();
        rs.shrink(maxHits);
        System.out.println("Search FACES " + sType + " filter " + fType + " -> hits " + rs.getSize() + " objs in " + reader.maxDoc() + " objs in " + (endTs - beginTs) + " ms.");

        return rs;
    }

    public void close() {
        for (LIndex li : indexs) {
            IndexWriter writer = li.writer;

            if (writer != null)
                try {
                    writer.commit();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            if (li.reader != null)
                try {
                    li.reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    public static void main(String[] args) {
        try {
            String str = "1011011110110110110110001101100111000001100000000011011000101110";
            Analyzer a = new NGramAnalyzer(8);

            TokenStream stream = a.tokenStream("content", new StringReader(str));
            OffsetAttribute offsetAttribute = stream.getAttribute(OffsetAttribute.class);
            CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);

            int n = 0;
            while (stream.incrementToken()) {
                int startOffset = offsetAttribute.startOffset();
                int endOffset = offsetAttribute.endOffset();
                String term = charTermAttribute.toString();
                System.out.println("TERM " + (n++) + ": " + term);
            }
            a.close();

            Analyzer b = new NGramAnalyzer(8);

            // Store the index in memory:
            Directory directory = new RAMDirectory();
            // To store an index on disk, use this instead:
            //Directory directory = FSDirectory.open("/tmp/testindex");
            IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_CURRENT, b);
            IndexWriter iwriter = new IndexWriter(directory, config);
            Document doc = new Document();

            doc.add(new Field("phash", str, TextField.TYPE_STORED));
            iwriter.addDocument(doc);
            iwriter.close();

            // Now search the index:
            DirectoryReader ireader = DirectoryReader.open(directory);
            IndexSearcher isearcher = new IndexSearcher(ireader);
            // Parse a simple query that searches for "text":
            //QueryParser parser = new QueryParser(Version.LUCENE_CURRENT, "phash", b);
            String q = "10110110";
            String q1 = "10110111";
            String q2 = "01101111";
            String q3 = "11011110";
            //Query query = parser.parse(q);
            PhraseQuery pq = new NGramPhraseQuery(8);
            pq.add(new Term("phash", q2), 0);
            pq.add(new Term("phash", q3));
            pq.rewrite(ireader);

            ScoreDoc[] hits = isearcher.search(pq, null, 1000).scoreDocs;
            // Iterate through the results:
            System.out.println("SRC: " + str);
            System.out.println("QRY: " + q);
            for (int i = 0; i < hits.length; i++) {
                Document hitDoc = isearcher.doc(hits[i].doc);
                System.out.println("HIT: " + hitDoc.get("phash"));
            }
            ireader.close();
            directory.close();
        } catch (IOException ie) {
            System.out.println("IO Error " + ie.getMessage());
        }
    }
}
