package iie.metastore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * Lucene File Filter do file filter based on user supplied 'field_name', 'filter_op', 
 * 'filter_type', 'filter_args'.
 * @author macan
 *
 */
public class LuceneFileFilter {

	public enum LFF_TYPE {
		LFF_TYPE_NUMBER, LFF_TYPE_STRING,
	};
	
	public static class Option {
	     String flag, opt;
	     public Option(String flag, String opt) { this.flag = flag; this.opt = opt; }
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<String> argsList = new ArrayList<String>();  
	    List<Option> optsList = new ArrayList<Option>();
	    List<String> doubleOptsList = new ArrayList<String>();
	    String file_path = null;
	    String fname = null;
	    LFF_TYPE ftype = null;
	    String fargs = null;
	    boolean delete = false, verbose = false;
	    String display_fields = null;
	    
		// parse the args
		for (int i = 0; i < args.length; i++) {
			//System.out.println("Args " + i + ", " + args[i]);
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
							optsList.add(new Option(args[i], args[i+1]));
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
				System.out.println("-h   : print this help.");
				System.out.println("-f   : lucene file path.");
				System.out.println("-n   : field name.");
				System.out.println("-t   : field type.");
				System.out.println("-args: field args.");
			}
			if (o.flag.equals("-f")) {
				// file path
				if (o.opt == null) {
					System.err.println("-f file_path");
					System.exit(0);
				}
				file_path = o.opt;
			}
			if (o.flag.equals("-n")) {
				// field name
				if (o.opt == null) {
					System.err.println("-n field_name");
					System.exit(0);
				}
				fname = o.opt;
			}
			if (o.flag.equals("-t")) {
				// field type
				if (o.opt == null) {
					System.err.println("-t field_type");
					System.exit(0);
				}
				if (o.opt.equalsIgnoreCase("number")) {
					ftype = LFF_TYPE.LFF_TYPE_NUMBER;
				} else if (o.opt.equalsIgnoreCase("string")) {
					ftype = LFF_TYPE.LFF_TYPE_STRING;
				}
			}
			if (o.flag.equalsIgnoreCase("-args")) {
				// field args
				if (o.opt == null) {
					System.err.println("-args field_args");
					System.exit(0);
				}
				fargs = o.opt;
			}
			if (o.flag.equals("-d")) {
				delete = true;
			}
			if (o.flag.equals("-v")) {
				verbose = true;
			}
			if (o.flag.equals("-df")) {
				// display fields
				if (o.opt == null) {
					System.err.println("-df display_fields");
					System.exit(0);
				}
				display_fields = o.opt;
			}
		}
		
		if (file_path == null || ftype == null || fname == null) {
			System.err.println("Args missing: need -f -t -n -args");
			System.exit(0);
		}
		
		try {
			Directory dir = FSDirectory.open(new File(file_path));
			Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_42);
			IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_42, analyzer);
			iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);

			IndexWriter writer = null;
			Query q = null;
			
			if (delete) 
				writer = new IndexWriter(dir, iwc);
			IndexReader reader = DirectoryReader.open(dir);
			IndexSearcher searcher = new IndexSearcher(reader);
			
			switch (ftype) {
			case LFF_TYPE_NUMBER:
				String[] x = fargs.split(",");
				long min = Long.MIN_VALUE, max = Long.MAX_VALUE;
				if (x.length == 1) {
					min = Long.parseLong(x[0]);
				} else if (x.length == 2) {
					min = Long.parseLong(x[0]);
					max = Long.parseLong(x[1]);
				}
				q = NumericRangeQuery.newLongRange(fname, min, max, true, true);
				break;
			case LFF_TYPE_STRING:
				q = new QueryParser(Version.LUCENE_42, fname, analyzer).parse(fargs);
				break;
			}
			ScoreDoc[] hits = null;
			if (reader.maxDoc() > 0) {
				hits = searcher.search(q, searcher.getIndexReader().maxDoc()).scoreDocs;
				System.out.println("Query '" + fargs + "' hits " + hits.length + " docs in " + 
						reader.maxDoc() + " docs.");
			} else {
				System.out.println("Query '" + fargs + "' hits 0 docs in " + reader.maxDoc() + " docs.");
			}
			if (verbose && hits != null && display_fields != null) {
				System.out.println("#Fields: " + display_fields);
				String[] fs = display_fields.split(",");
				String line = "| ";
				for (int i = 0; i < hits.length; i++) {
					Document d = reader.document(hits[i].doc);
					for (int j = 0; j < fs.length; j++) {
						line += d.get(fs[j]) + " | ";
					}
				}
				line += "\n";
				System.out.println(line);
			}
			if (delete) {
				writer.deleteDocuments(q);
				writer.commit();
				writer.close();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

}
