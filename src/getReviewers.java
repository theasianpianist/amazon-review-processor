import java.sql.*;
import java.util.ArrayList;

/**
 * Created by LFQLE on 2/24/2017.
 */
public class getReviewers {
	public static void getReviewers (String dbPath, String reviewerPath) {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection dbConn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
				 PreparedStatement getReviews = dbConn.prepareStatement("SELECT * FROM reviews");
				 ResultSet reviews = getReviews.executeQuery();
				 Connection reviewerConn = DriverManager.getConnection("jdbc:sqlite:" + reviewerPath);
				 PreparedStatement insertReviewer = reviewerConn.prepareStatement("INSERT INTO reviewers (reviewerID) values (?1)")) {
				ArrayList<String> reviewers = new ArrayList<>();
				while (reviews.next()) {
					String reviewerID = reviews.getString(1);
					if (!reviewers.contains(reviewerID)) {
						reviewers.add(reviews.getString(1));
					}
				}
				int x = 1;
				for (String reviewer : reviewers) {
					insertReviewer.setString(1, reviewer);
					insertReviewer.addBatch();
					if (x % 100 == 0) {
						insertReviewer.executeBatch();
						System.out.println(x);
					}
					x++;
				}
				insertReviewer.executeBatch();
				System.out.println(x);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main (String[] args) {
		getReviewers("Z:\\RAM_itemDB\\reviews.db", "Z:\\RAM_itemDB\\reviewers.db");
	}
}
