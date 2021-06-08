

import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;
import org.bson.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.function.Consumer;
import java.lang.Math; 

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class Indexer {

	private static MongoClient mongo;
    private static MongoDatabase mongoDatabase;
    private static MongoCollection<Document> mongoCollection_URL;
    private static MongoCollection<Document> mongoCollection_Indexer;
    
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws FileNotFoundException {
		mongo = new MongoClient("localhost", 27017);
		mongoDatabase = mongo.getDatabase("SearchEngine");
		mongoCollection_URL = mongoDatabase.getCollection("Crawler");
		mongoCollection_Indexer = mongoDatabase.getCollection("Indexer");
		SnowballStemmer stemmer = new englishStemmer();
		ArrayList<String> stopwordsList = loadStopwords();
		ArrayList<Document> URL_Documents = new ArrayList<Document>();
		ArrayList<String> urlList = new ArrayList<String>();
		ArrayList<String> titlesList = new ArrayList<String>();
		ArrayList<String> contentList = new ArrayList<String>();
		MongoCursor<Document> cursor = mongoCollection_URL.find().iterator();
		try {
		    while (cursor.hasNext()) {
		        URL_Documents.add(cursor.next());
		    }
		} finally {
		    cursor.close();
		}
		for(Document document : URL_Documents) {
			urlList.add((String) document.get("URL"));
			contentList.add((String) document.get("Content"));
			titlesList.add((String) document.get("Title"));
		}
		for(int i = 0; i < urlList.size(); i++) {
			String urlContent = contentList.get(i);
			urlContent = urlContent.replaceAll("[^a-zA-Z0-9]", " ");
			urlContent = urlContent.replaceAll("\\s+", " ");
			String[] urlContents = urlContent.split(" ");
			ArrayList<String> contentsList = new ArrayList<String>();
			for(int k = 0; k < urlContents.length; k++) {
				contentsList.add(urlContents[k].toLowerCase());
			}
			contentsList.removeAll(stopwordsList);
			for (int j = 0; j < contentsList.size(); j++) {
				stemmer.setCurrent(contentsList.get(j));
				stemmer.stem();
				contentsList.set(j, stemmer.getCurrent());
			}
			Set<String> uniqueWordSet = new HashSet<String>(contentsList);
			uniqueWordSet.remove("");
			uniqueWordSet.remove(" ");
			for(String word:uniqueWordSet) {
				BasicDBObject wordQuery = new BasicDBObject("Word", word);
			    FindIterable<Document> findWord = mongoCollection_Indexer.find(wordQuery);
			    if (findWord.cursor().hasNext()) {
			    	BasicDBObject newObject = new BasicDBObject();
			    	newObject.append("Website", urlList.get(i))
			    	.append("Title", titlesList.get(i))
		        	.append("Count", Collections.frequency(contentsList, word))
		        	.append("TotalCount", contentsList.size())
		        	.append("TF-IDF", ((double) Collections.frequency(contentsList, word) / (double) contentsList.size()));
			    	mongoCollection_Indexer.updateOne(wordQuery, new BasicDBObject("$push", new BasicDBObject("Information", newObject)));
			    
			    }
			    else {
			    	Document newDocument = new Document("Word", word);
			    	mongoCollection_Indexer.insertOne(newDocument);
			    	BasicDBObject newWordQuery = new BasicDBObject("Word", word);
			    	BasicDBObject newObject = new BasicDBObject();
			    	newObject.append("Website", urlList.get(i))
			    	.append("Title", titlesList.get(i))
		        	.append("Count", Collections.frequency(contentsList, word))
		        	.append("TotalCount", contentsList.size())
		        	.append("TF-IDF", ((double) Collections.frequency(contentsList, word) / (double) contentsList.size()));
			    	mongoCollection_Indexer.updateOne(newWordQuery, new BasicDBObject("$push", new BasicDBObject("Information", newObject)));
			    }
			}
		}

		long totalDocumentsNumber = mongoCollection_URL.countDocuments();
		MongoCursor<Document> documentsCursor1 = mongoCollection_Indexer.find().cursor();
		MongoCursor<Document> documentsCursor2 = mongoCollection_Indexer.find().cursor();
		try {
			while(documentsCursor1.hasNext()) {
				String word = (String) documentsCursor1.next().get("Word");
				@SuppressWarnings("unchecked")
				int numDocumentsContainingWord = ((ArrayList<BasicDBObject>) documentsCursor2.next().get("Information")).size();
				double IDF = Math.log10((double) totalDocumentsNumber / (double) numDocumentsContainingWord);
				BasicDBObject objectWord = new BasicDBObject("Word", word);
				BasicDBObject objectIDF = new BasicDBObject("IDF", IDF);
		    	mongoCollection_Indexer.updateOne(objectWord, new BasicDBObject("$set", objectIDF));
			}
		}
		finally {
			documentsCursor2.close();
			documentsCursor1.close();
		}

		ArrayList<Document> wordDocuments = new ArrayList<Document>();
		cursor = mongoCollection_Indexer.find().iterator();
		try {
		    while (cursor.hasNext()) {
		    	wordDocuments.add(cursor.next());
		    }
		} finally {
		    cursor.close();
		}
		mongoCollection_Indexer.find().forEach(new Block<Document>() {
            @Override
            public void apply(Document document) {
                final double IDF = document.getDouble("IDF");
                @SuppressWarnings("unchecked")
				ArrayList<Document> info = (ArrayList<Document>) document.get("Information");
                info.forEach(new Consumer<Document>() {
                    @Override
                    public void accept(Document updatedDocument) {
                        double TFIDF = 0;
                        try {
                            TFIDF = (Double) updatedDocument.get("TF-IDF");
                        } catch (ClassCastException e) {
                            TFIDF = 1.0d;
                        }
                        TFIDF =TFIDF* IDF;
                        updatedDocument.put("TF-IDF", TFIDF);
                    }
                });
                BasicDBObject objectWord = new BasicDBObject("Word", document.getString("Word"));
                BasicDBObject objectInfo = new BasicDBObject("Information", info);
                mongoCollection_Indexer.findOneAndUpdate(objectWord, new BasicDBObject("$set", objectInfo));
            }
        });
		mongo.close();
	}
	
	public static ArrayList<String> loadStopwords() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File("/Users/amrkhaled/Documents/eclipse_workspace/APT_Interface/src/main/java/StopWords.txt"));
		ArrayList<String> list = new ArrayList<String>();
		while (scanner.hasNext()){
		    list.add(scanner.next().toLowerCase());
		}
		scanner.close();
		return list;
	}
}