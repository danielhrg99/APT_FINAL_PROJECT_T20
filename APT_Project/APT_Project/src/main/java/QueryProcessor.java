package project;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;

import jakarta.servlet.annotation.WebServlet;

import org.bson.Document;
import org.tartarus.snowball.SnowballStemmer;
import org.tartarus.snowball.ext.englishStemmer;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.json.simple.JSONObject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static com.mongodb.client.model.Updates.set;

@WebServlet("/QueryProcessor")
public class QueryProcessor extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private final int SNIPPET_LENGTH = 300;
	private final int MAX_NUM_OF_RESULTS = 200;
	private final int NUM_RESULTS_PER_PAGE = 200;

	ArrayList<String> StopWords;
	private MongoDatabase Database;
	private MongoCollection<Document> Words;
	private MongoCollection<Document> Websites;
	private MongoCollection<Document> Results;

	boolean local = true;

	String url ="dummy";
	
	Block<Document> printBlock = new Block<Document>() {
		@Override
		public void apply(final Document document) {
			System.out.println(document.toJson());
		}
	};

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) {

    resp.setContentType("text/html");


		//apply query processor
		String arr = WordStem(req.getParameter("SearchWord"));

		if (arr== "") {
			// if array length is "" then the word being searched is a stop word
			Document responseDoc = new Document("Status", 0);
			String response = responseDoc.toJson();
			
		} else {
			initializeDb();

			search(arr);

			String response;

			//return status = 0 if there is no results returned
			if (Results.count() == 0) {
				response = new Document("Status", 0).toJson();
			} else {
				Document responseDoc = new Document("Status", 1);
				responseDoc.append("Number of Results", (int) Results.count());
				responseDoc.append("Results", Results.find()
						.sort(Sorts.descending("Relevance"))
						.limit(NUM_RESULTS_PER_PAGE)
						.projection(Projections.exclude("Relevance")));
				response = responseDoc.toJson();
			}

			try {
		         FileWriter file = new FileWriter("/Users/amrkhaled/Documents/eclipse_workspace/APT_Interface/src/main/webapp/Result.json");
		         file.write(response);
		         file.close();
		         Path path = Paths.get("/Users/amrkhaled/Documents/eclipse_workspace/APT_Interface/src/main/webapp/Result.html");
		         String lines = Files.readString(path);
		         resp.getWriter().println(lines);
		      } catch (IOException e) {
		         // TODO Auto-generated catch block
		         e.printStackTrace();
		      }
		}
	}
	
	void initializeDb() {
		long startTime = System.currentTimeMillis();
		MongoClient mongoClient;
		if (!local) {
			MongoClientURI clientURI = new MongoClientURI(url);
			mongoClient = new MongoClient(clientURI);
		} else {
			mongoClient = new MongoClient("localhost", 27017);
		}
		Database = mongoClient.getDatabase("SearchEngine");
		Websites = Database.getCollection("Crawler");
		Words = Database.getCollection("Indexer");
		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Database Initialization Duration = " + duration);
	}

	public String WordStem(String word) {
		long startTime = System.currentTimeMillis();
		String searchWord = word.toLowerCase();
		StopWordsLoad();
		if(StopWords.contains(searchWord))
			return "";
		SnowballStemmer snowballStemmer = new englishStemmer();
		snowballStemmer.setCurrent(searchWord);
		snowballStemmer.stem();
		searchWord=snowballStemmer.getCurrent();

		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Process Query Duration = " + duration);
		System.out.println("\n");
		return searchWord;
	}


	public void StopWordsLoad() {
		long startTime = System.currentTimeMillis();
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File("StopWords.txt"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		StopWords = new ArrayList<String>();
		while (scanner.hasNext()) {
			StopWords.add(scanner.next());
		}
		scanner.close();
		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Loading Stop Words Duration = " + duration);
	}


	public void search(String word) {
		long startTime = System.currentTimeMillis();

		getMatches(word);
		Snippet(word);

		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Total Duration = " + duration);
		System.out.println("\n");

		//print the results
		Results.find().sort(Sorts.descending("Relevance")).projection(Projections.excludeId()).forEach(printBlock);
		System.out.println("Number of results: " + Results.count());
	}


	private void Snippet( final String Query) {
		long startTime = System.currentTimeMillis();

		Results.updateMany(new Document(), set("Snippet", ""));
		Results.find().forEach(new Block<Document>() {
			@Override
			public void apply(Document document) {
				String content = Websites.find(Filters.eq("URL", document.getString("_id")))
						.first().getString("Content");

				//start by the first word in the query and if found make snippet from first word.
				int index = content.toLowerCase().indexOf(Query);
				String Content = "";
					try {
						Content = content.substring(index, index + SNIPPET_LENGTH);
					} catch (StringIndexOutOfBoundsException e) {
						Content = content.substring(index);
					}

				Document update = new Document(document);
				update.put("Snippet", Content);
				Results.replaceOne(document, update);
			}
		});
		System.out.println("Snippet Results");
		Results.find().projection(Projections.excludeId()).forEach(printBlock);
		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Snippet Function Duration = " + duration);
	}

	private void getMatches(String word) {
		long startTime = System.currentTimeMillis();

		Words.aggregate(
				Arrays.asList(
						Aggregates.match(Filters.eq("Word", word)),
						Aggregates.unwind("$Information"),
						Aggregates.group("$Information.Website", Accumulators.first("Relevance", "$Information.TF-IDF"),
								Accumulators.first("Title", "$Information.Title"),
								Accumulators.first("Date", "$Information.Date")),
						Aggregates.sort(Sorts.descending("Relevance")),
						Aggregates.limit(MAX_NUM_OF_RESULTS),
						Aggregates.out("Results")
						)).toCollection();

		Results = Database.getCollection("Results");

		long duration = System.currentTimeMillis() - startTime;
		System.out.println("Results: " + word);
		Results.find().projection(Projections.excludeId()).forEach(printBlock);
		System.out.println("Results Duration = " + duration);
	}


}
