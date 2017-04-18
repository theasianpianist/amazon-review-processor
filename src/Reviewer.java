import java.util.ArrayList;

/**
 * Created by LFQLE on 2/16/2017.
 */
public class Reviewer {
	String reviewerID;
	int totalVotes = 0;
	int helpfulVotes = 0;
	int numReviews = 0;
	double totalStars = 0;
	public Reviewer(String id, Object[] review) {
		reviewerID = id;
		addReview(review);
	}

	public void addReview(Object[] review) {
		helpfulVotes += Integer.parseInt((String) review[5]);
		totalVotes += Integer.parseInt((String) review[6]);
		numReviews ++;
		totalStars += (double) review[2];
	}

	public int numReviews() { return numReviews; }

	public int totalVotes() { return totalVotes;}

	public int helpfulVotes() { return helpfulVotes;}

	public double avgRating() { return totalStars / numReviews;	}

	public String getReviewerID() {
		return reviewerID;
	}
}
