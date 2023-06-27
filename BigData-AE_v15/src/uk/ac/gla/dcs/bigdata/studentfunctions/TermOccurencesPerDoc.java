package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.function.FlatMapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.TermOccurenceCount;
import uk.ac.gla.dcs.bigdata.studentstructures.TokenizedNewsMap;

public class TermOccurencesPerDoc implements FlatMapFunction<TokenizedNewsMap, TermOccurenceCount> {

	private static final long serialVersionUID = -1708630265730954316L;
	int docOccurence = 0;
	int totalOccurence;
	List<String> query;
	NewsArticle article;

	public TermOccurencesPerDoc(List<String> query) {
		this.query = query;
	}
	//returns TermOccurenceCOunt object if NewsArticle contains term in the query. Maps 1 to many relationship
	@Override
	public Iterator<TermOccurenceCount> call(TokenizedNewsMap t) throws Exception {
		// TODO Auto-generated method stub
		List<TermOccurenceCount> occCountList = new ArrayList<TermOccurenceCount>();
		//for each term in the query, checks if term is present and counts frequency
		for (int i = 0; i < query.size(); i++) {

			docOccurence = 0;
			for (int j = 0; j < t.getContentTokenized().size(); j++) {

				if (t.getContentTokenized().get(j).equals(query.get(i))) {
					docOccurence++;
				}
			}
			// if the term is present, creates a new object for the term with the doc occurence count
			if (docOccurence > 0) {
				TermOccurenceCount queryTerm = new TermOccurenceCount(t.getId(), docOccurence, query.get(i),
						(int) t.getWordCount());
				queryTerm.setArticle(t.getArticle());
				occCountList.add(queryTerm);

			}

		}
		//returns term occurence count objects
		if (occCountList.size() > 0) {

			return occCountList.iterator();
		}
		// returns nothing if term is not present in new article
		else {
			List<TermOccurenceCount> emptyList = new ArrayList<TermOccurenceCount>(0);
			return emptyList.iterator();

		}
	}
}
