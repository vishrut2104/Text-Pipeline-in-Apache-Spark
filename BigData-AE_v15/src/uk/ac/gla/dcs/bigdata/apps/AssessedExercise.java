package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.AverageDPHPerQuery;
import uk.ac.gla.dcs.bigdata.studentfunctions.KeyNewsByID;
import uk.ac.gla.dcs.bigdata.studentfunctions.KeyTermdatasetByTerm;
import uk.ac.gla.dcs.bigdata.studentfunctions.Tokenizer;
import uk.ac.gla.dcs.bigdata.studentfunctions.CalculateDPHPerTerm;
import uk.ac.gla.dcs.bigdata.studentfunctions.TermOccurencesPerDoc;
import uk.ac.gla.dcs.bigdata.studentfunctions.NumOfTotalTermOccurences;
import uk.ac.gla.dcs.bigdata.studentstructures.TermOccurenceCount;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsMap;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overridden by the
 * spark.master environment variable.
 * 
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) {

		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get
														// an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark
																			// finds it

		// The code submitted for the assessed exerise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef == null)
			sparkMasterDef = "local[2]"; // default is local mode with two executors

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile == null)
			queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		
		//The file is really large so it might not be present in the zipped version of the code depending on which version you have.
		// Might have to paste the dataset manually in the data folder.
		
		String newsFile = System.getenv("bigdata.news");
		if (newsFile == null)
			newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix 2.json"; // default is a sample of 5000 news
																				// articles

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		// Check if the code returned any results
		if (results == null)
			System.err
					.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// We have set of output rankings, lets write to disk

			// Create a new folder
			File outDirectory = new File("results/" + System.currentTimeMillis());
			if (!outDirectory.exists())
				outDirectory.mkdir();

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}

	}

	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java
		// objects
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts
																										// each row into
																										// a Query
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this
																											// converts
																											// each row
																											// into a
																											// NewsArticle

		// ----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// ----------------------------------------------------------------

		// variables to keep track of number of words in each document/all docs

		List<Query> query = queries.collectAsList();

		List<DocumentRanking> rankDocs = new ArrayList<DocumentRanking>();

		for (int i = 0; i < query.size(); i++) {

			LongAccumulator wordCountAccumulator = spark.sparkContext().longAccumulator();
			LongAccumulator numberofdocsAccumulator = spark.sparkContext().longAccumulator();
			LongAccumulator totalwordCountAccumulator = spark.sparkContext().longAccumulator();

			Tokenizer tokenMap = new Tokenizer(wordCountAccumulator, numberofdocsAccumulator,
					totalwordCountAccumulator);

			//Tokenizing title/content of news article, and removing articles with empty title or contents
			//tokenizedNews is the entire dataset with token values of title and contents in the same column
			Dataset<TokenizedNewsMap> tokenizedNews = news.flatMap(tokenMap, Encoders.bean(TokenizedNewsMap.class));

			tokenizedNews.show();
			double averageWordCount = (totalwordCountAccumulator.value() * 1.0) / (numberofdocsAccumulator.value());

			TermOccurencesPerDoc countOccurencesPerDoc = new TermOccurencesPerDoc(query.get(i).getQueryTerms());
			
			//termDataset is tokenizedNews with the added column of query term count in each document.
			Dataset<TermOccurenceCount> termDataset = tokenizedNews.flatMap(countOccurencesPerDoc,Encoders.bean(TermOccurenceCount.class));
			
			//keyByTerm is term dataset with the key,value group of term,row.
			KeyTermdatasetByTerm keyByTerm = new KeyTermdatasetByTerm();
			KeyValueGroupedDataset<String, TermOccurenceCount> datasetbyTerm = termDataset.groupByKey(keyByTerm,
					Encoders.STRING());
			
			
			NumOfTotalTermOccurences sum = new NumOfTotalTermOccurences();
			
			//mapGroup function to calculate the sum of term occurenecs column.
			Dataset<Tuple2<String, Integer>> termCount = datasetbyTerm.mapGroups(sum,
					Encoders.tuple(Encoders.STRING(), Encoders.INT()));
			
			//defining a dictionary to store the query term and it's total count across all documents.
			Map<String, Integer> DocTermDict = new HashMap<>();
			for (Tuple2<String, Integer> row : termCount.collectAsList()) {
				DocTermDict.put(row._1, row._2);
			}
			
			//Calculating the DPH per term for each article.
			CalculateDPHPerTerm calcDPH = new CalculateDPHPerTerm(averageWordCount, numberofdocsAccumulator.value(), DocTermDict);
			Dataset<TermOccurenceCount> termDPHDataset = termDataset.map(calcDPH, Encoders.bean(TermOccurenceCount.class));
			
			//Grouping each term by Article ID.
			KeyNewsByID keyByID = new KeyNewsByID();
			KeyValueGroupedDataset<String, TermOccurenceCount> NewsByID = termDPHDataset.groupByKey(keyByID,
					Encoders.STRING());
			
			//Grouping the article by term calculate average DPH.
			AverageDPHPerQuery avg = new AverageDPHPerQuery(query.get(i));

			//Maps dataset to RankedResult type object.
			Dataset<RankedResult> result = NewsByID.mapGroups(avg, Encoders.bean(RankedResult.class));
			
			//Sorting the DPH score to get the top 1000 rows.
			Dataset<RankedResult> sortedResult = result.sort(functions.desc("score")).limit(1000);
			
			//Converting the sorted rows to an Array list.
			List<RankedResult> results = sortedResult.collectAsList();
			ArrayList<RankedResult> topResults = new ArrayList<RankedResult>(results);
			
			//Returns top 10 rows based on title distances.
			List<RankedResult> topTen = getTopTen(topResults);
			
			//Creating new document ranking object and adding it to list.
			DocumentRanking d = new DocumentRanking(query.get(i), topTen);
			System.out.println(d.toString());
			rankDocs.add(d);

			wordCountAccumulator.reset();
			numberofdocsAccumulator.reset();
			totalwordCountAccumulator.reset();
		}

		return rankDocs; // replace this with the the list of DocumentRanking output by your topology
	}

	
	// recursive function to compare titles between articles
	public static List<RankedResult> getTopTen(ArrayList<RankedResult> results) {

		for (int i = 0; i < 10; i++) {
			for (int k = i + 1; k < 10; k++) {
				String r1 = results.get(i).getArticle().getTitle();
				String r2 = results.get(k).getArticle().getTitle();
				double dist = TextDistanceCalculator.similarity(r1, r2);
				//if the distance is below 0.50, remove Ranked Result with lower DPH score
				if (dist < 0.50) {
					double score1 = results.get(i).getScore();
					double score2 = results.get(k).getScore();

					if (Math.min(score1, score2) == score1) {
						results.remove(i);

						return getTopTen(results);
					} else {
						results.remove(k);

						return getTopTen(results);
					}

				}

			}
		}
		//returns 10 results without distance below 0.50
		return results.subList(0, 10);
	}

}
