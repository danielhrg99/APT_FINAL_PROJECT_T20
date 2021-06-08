
import java.io.File;
import java.io.FileNotFoundException;
//Java imports
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
//jsoup imports
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.bson.Document;
// MongoDB imports
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class Crawler implements Runnable {
    private static MongoClient mongo;
    private static MongoDatabase mongoDatabase;
    private static MongoCollection<Document> mongoCollection_URL;
    
	Crawler() { }
    
	@Override
	public void run() {
		try {
			Crawl(Thread.currentThread().getName());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static ArrayList<String> loadSeedSet() throws FileNotFoundException {
		Scanner scanner = new Scanner(new File("/Users/amrkhaled/Documents/eclipse_workspace/APT_Interface/src/main/java/SeedSet.txt"));
		ArrayList<String> list = new ArrayList<String>();
		while (scanner.hasNext()){
		    list.add(scanner.next().toLowerCase());
		}
		scanner.close();
		return list;
	}

		

	public static void main(String[] args) throws InterruptedException, IOException {
		mongo = new MongoClient("localhost", 27017);
		mongoDatabase = mongo.getDatabase("SearchEngine");
		mongoCollection_URL = mongoDatabase.getCollection("Crawler");
		ArrayList<String> SeedSetList = loadSeedSet();
		System.out.println("Please Enter Number Of threads:");
		Scanner sc= new Scanner(System.in);
		int ThreadsNum = sc.nextInt();
		ArrayList<Thread> ThreadsList= new ArrayList<Thread>();
		
		for(int i=0; i<ThreadsNum;i++)
		{
			ThreadsList.add(new Thread(new Crawler()));
		}
		int count=0;
		long startTime = System.currentTimeMillis();
		int FirstTime=0;
		for(int i=0;i<SeedSetList.size();)
		{
			for(int j=0; (j<ThreadsNum) && (i<SeedSetList.size());j++)
			{
				ThreadsList.get(j).setName(SeedSetList.get(i));
				if(FirstTime<ThreadsNum)
				{
				ThreadsList.get(j).start();
				FirstTime++;
				}
				i++;
				count=j+1;
			}
			for(int k=0; k<count;k++)
			{
				ThreadsList.get(k).join();
			}
		}
	
		long endTime = System.currentTimeMillis()-startTime;
		System.out.println("Crawling time =" + ((int) ((endTime / (1000*60)) % 60)) + " minutes");
		startTime = System.currentTimeMillis();
		consistencyFunction();
		endTime = System.currentTimeMillis()-startTime;
		System.out.println("Filling Null fields in Duplicated Urls & crawledOutLinks Process took " + ((int) ((endTime / (1000*60)) % 60)) + " minutes");
		mongo.close();
	}
	
	//robots.txt check
	public static boolean checkRobot(String url) throws IOException {
		if(!validUrl(url)) {
			return false;
		}
		URL urlObject = new URL(url);
		String Robot = urlObject.getProtocol() + "://" + urlObject.getHost() + "/robots.txt";
		org.jsoup.nodes.Document document = Jsoup.connect(Robot).get();
		String urlBody = document.body().text();
		int startIndex = urlBody.indexOf("User-agent: *");
		int stopIndex = urlBody.indexOf("User-agent:", startIndex + 13);
		//Check if no (User-agent: *) exists in which case is us.
		if(startIndex == -1) {
			return true;
		}
		//Check if website denies access to all its contents
		int denyAll = urlBody.indexOf("Disallow: / ", startIndex + 13);
		if(denyAll != -1) {
			if(stopIndex != -1) {
				if(denyAll < stopIndex) {
					return false;
				}
			}
			else {
				return false;
			}
		}
		if(urlObject.getPath().equals("") || urlObject.getPath().equals("/")) {
			return true;
		}
		//Check if the given URL is valid to be crawled
		int isValid = urlBody.indexOf("Disallow: " + urlObject.getPath() + " ", startIndex + 13);
		if(isValid != -1) {
			if(stopIndex != -1) {
				if(isValid < stopIndex) {
					return false;
				}
			}
			else {
				return false;
			}
		}
		return true;
	}
	
	//checks if current page (before crawling) if it exists in DB
	public static boolean inDatabase(String url, String content) {
		  BasicDBObject urlQuery = new BasicDBObject("URL", url);
	      BasicDBObject contentQuery = new BasicDBObject("Content", content);
	      FindIterable<Document> urlCursor = mongoCollection_URL.find(urlQuery);
	      FindIterable<Document> contentCursor = mongoCollection_URL.find(contentQuery);
	      if(urlCursor.cursor().hasNext() || contentCursor.cursor().hasNext()) {
              BasicDBObject updateObject = new BasicDBObject("$push", new BasicDBObject("DuplicatedUrls", url));
              mongoCollection_URL.updateOne(contentQuery, updateObject);
	    	  return true;
	      }
	      return false;
	}
	
	//URLs which are not in the specified format will generate an error if converted to URI format.
	public static boolean validUrl(String url) 
    { 
        try { 
            new URL(url).toURI(); 
            return true; 
        } 
        catch (Exception e) { 
            return false; 
        } 
    }


	public static void consistencyFunction() {
		//Handling crawledOutLinks 
				MongoCursor<Document> crawledoutLinks = mongoCollection_URL.find(new BasicDBObject("crawledOutLinks", new BasicDBObject("$exists", false))).cursor();
				try {
					while (crawledoutLinks.hasNext()) {
						BasicDBObject contentQuery = new BasicDBObject("Content", crawledoutLinks.next().get("Content"));
						BasicDBObject newDocument = new BasicDBObject("crawledOutLinks", null);
			            BasicDBObject update = new BasicDBObject("$set", newDocument);
			            mongoCollection_URL.updateOne(contentQuery, update);
					}
				} finally {
					crawledoutLinks.close();
				}
		//Handling DuplicatedUrls 
		MongoCursor<Document> duplicateUrl = mongoCollection_URL.find(new BasicDBObject("DuplicatedUrls", new BasicDBObject("$exists", false))).cursor();
		try {
			while (duplicateUrl.hasNext()) {
				BasicDBObject contentQuery = new BasicDBObject("Content", duplicateUrl.next().get("Content"));
	            BasicDBObject update = new BasicDBObject("$set", new BasicDBObject("DuplicatedUrls", null));
	            mongoCollection_URL.updateOne(contentQuery, update);
			}
		} finally {
			duplicateUrl.close();
		}
		
	}
	
	
	public static void Crawl(String url) throws IOException {
    	if (!validUrl(url) || !checkRobot(url) || mongoCollection_URL.countDocuments() > 5000  ) {
    		return;
    	}
		try {
            org.jsoup.nodes.Document jsoupDocument = Jsoup.connect(url).get();
    		if (!inDatabase(url, jsoupDocument.body().text())) {
    			Elements linksExtraced = jsoupDocument.select("a[href]");
            	Document crawlerDocument = new Document("URL", url)
				.append("Content", jsoupDocument.body().text())
				.append("Title", jsoupDocument.title());
                mongoCollection_URL.insertOne(crawlerDocument);
                
                BasicDBObject urlQuery = new BasicDBObject();
                urlQuery.put("URL" , url);
                BasicDBObject newDocument = new BasicDBObject();
                if(linksExtraced.eachAttr("abs:href").isEmpty()) {
                    newDocument.put("crawledOutLinks", null);
                }
                else {
                	Set<String> links = new HashSet<String>(linksExtraced.eachAttr("abs:href"));
                	newDocument.put("crawledOutLinks", links);
                }
                BasicDBObject updateObject = new BasicDBObject("$set", newDocument);
      			mongoCollection_URL.updateOne(urlQuery, updateObject);
                for (Element page : linksExtraced) {
                    Crawl(page.attr("abs:href"));
                }
            }
    	}
		catch(NullPointerException e) {
    		System.err.println("For '" + url + "': " + e.getMessage());
    		return;
    	}
		catch (IOException e) {
            System.err.println("For '" + url + "': " + e.getMessage());
            return;
        }
    }
}