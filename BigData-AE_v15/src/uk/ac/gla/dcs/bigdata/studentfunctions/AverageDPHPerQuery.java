package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Iterator;
import org.apache.spark.api.java.function.MapGroupsFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.TermOccurenceCount;

public class AverageDPHPerQuery implements MapGroupsFunction<String, TermOccurenceCount, RankedResult> {

	private static final long serialVersionUID = -6456363146611418557L;
	Query q;
	NewsArticle article;

	public AverageDPHPerQuery(Query q) {
		this.q = q;
	}

	// Returns average DPH by totalling all DPH for articles with term in the query then dividing by no. of query terms
	@Override
	public RankedResult call(String key, Iterator<TermOccurenceCount> values) throws Exception {

		RankedResult result = new RankedResult();
		double averageDPH = 0.0;
		NewsArticle n = new NewsArticle();
		int count = q.getQueryTerms().size();
		while (values.hasNext()) {
			TermOccurenceCount term = values.next();
			averageDPH = averageDPH + term.getDPHScore();

			n = term.getArticle();
		}

		averageDPH = averageDPH / count;

		result.setScore(averageDPH);
		result.setDocid(key);
		result.setArticle(n);
		return result;
	}

}
