import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import static java.lang.Double.NaN;

/**
 * Created by LFQLE on 2/22/2017.
 */
public class ItemCalc {
	private String reviewerPath;
	private String itemPath;
	private String reviewPath;
	public ItemCalc(String reviewerDBPath, String itemDBPath, String reviewDBPath) {
		reviewerPath = reviewerDBPath;
		itemPath = itemDBPath;
		reviewPath = reviewDBPath;
	}

	public void calculateAvgRating() {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection itemConn = DriverManager.getConnection("jdbc:sqlite:" + itemPath);
				 PreparedStatement itemStmt = itemConn.prepareStatement("SELECT asin FROM items");
				 PreparedStatement insertRating = itemConn.prepareStatement("UPDATE items SET originalRating = ? WHERE asin = ?");
				 ResultSet items = itemStmt.executeQuery();
				 Connection reviewConn = DriverManager.getConnection("jdbc:sqlite:" + reviewPath);
				 PreparedStatement reviewStmt = reviewConn.prepareStatement("SELECT reviewerID, rating FROM reviews WHERE asin = ?");
				 Connection reviewerConn = DriverManager.getConnection("jdbc:sqlite:" + reviewerPath);
				 PreparedStatement reviewerStmt = reviewerConn.prepareStatement("SELECT classification FROM reviewers WHERE reviewerID = ?")) {
				int x = 1;
				while (items.next()) {
					double rating = 0;
					int numReviews = 0;
					String asin = items.getString("asin");
					reviewStmt.setString(1, asin);
					ResultSet reviews = reviewStmt.executeQuery();
					while (reviews.next()) {
						String reviewerID = reviews.getString("reviewerID");
						reviewerStmt.setString(1, reviewerID);
						ResultSet reviewer = reviewerStmt.executeQuery();
						String classification = reviewer.getString("classification");
						rating += reviews.getDouble("rating");
						numReviews++;
						reviewer.close();
					}
					reviews.close();
					insertRating.setDouble(1, rating / numReviews);
					insertRating.setString(2, asin);
					insertRating.addBatch();
					if (x % 100 == 0) {
						insertRating.executeBatch();
						System.out.println("Added adjusted ratings for " + x + " items");
					}
					x++;
				}
				insertRating.executeBatch();
				System.out.println("Added adjusted ratings for " + x + " items");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void calculateAdjRating() {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection itemConn = DriverManager.getConnection("jdbc:sqlite:" + itemPath);
				 PreparedStatement itemStmt = itemConn.prepareStatement("SELECT asin FROM items");
				 PreparedStatement insertRating = itemConn.prepareStatement("UPDATE items SET adjustedRating = ? WHERE asin = ?");
				 ResultSet items = itemStmt.executeQuery();
				 Connection reviewConn = DriverManager.getConnection("jdbc:sqlite:" + reviewPath);
				 PreparedStatement reviewStmt = reviewConn.prepareStatement("SELECT reviewerID, rating FROM reviews WHERE asin = ?");
				 Connection reviewerConn = DriverManager.getConnection("jdbc:sqlite:" + reviewerPath);
				 PreparedStatement reviewerStmt = reviewerConn.prepareStatement("SELECT classification FROM reviewers WHERE reviewerID = ?")) {
				int x = 1;
				while (items.next()) {
					double rating = 0;
					int numReviews = 0;
					String asin = items.getString("asin");
					reviewStmt.setString(1, asin);
					ResultSet reviews = reviewStmt.executeQuery();
					while (reviews.next()) {
						String reviewerID = reviews.getString("reviewerID");
						reviewerStmt.setString(1, reviewerID);
						ResultSet reviewer = reviewerStmt.executeQuery();
						String classification = reviewer.getString("classification");
						if (classification.equals("R")) {
							rating += reviews.getDouble("rating");
							numReviews++;
						}
						reviewer.close();
					}
					reviews.close();
					insertRating.setDouble(1, rating / numReviews);
					insertRating.setString(2, asin);
					insertRating.addBatch();
					if (x % 100 == 0) {
						insertRating.executeBatch();
						System.out.println("Added adjusted ratings for " + x + " items");
					}
					x++;
				}
				insertRating.executeBatch();
				System.out.println("Added adjusted ratings for " + x + " items");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void calculateAlphas() {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection itemConn = DriverManager.getConnection("jdbc:sqlite:" + itemPath);
				 PreparedStatement itemStmt = itemConn.prepareStatement("SELECT asin, originalRating, adjustedRating FROM items");
				 ResultSet items = itemStmt.executeQuery();
				 Connection reviewConn = DriverManager.getConnection("jdbc:sqlite:" + reviewPath);
				 PreparedStatement reviewStmt = reviewConn.prepareStatement("SELECT reviewerID, rating FROM reviews WHERE asin = ?");
				 Connection reviewerConn = DriverManager.getConnection("jdbc:sqlite:" + reviewerPath);
				 PreparedStatement reviewerStmt = reviewerConn.prepareStatement("SELECT classification FROM reviewers WHERE reviewerID = ?")) {
				int x = 1;
				while (items.next()) {
					double rating = 0;
					int numReviews = 0;
					String asin = items.getString("asin");
					double originalRating = items.getDouble("originalRating");
					double adjustedRating = items.getDouble("adjustedRating");
					reviewStmt.setString(1, asin);
					int column = 0;
					if (x > 2000) {
						for (double alpha = 0; alpha <= 1; alpha += .1) {
							ResultSet reviews = reviewStmt.executeQuery();
							while (reviews.next()) {
								String reviewerID = reviews.getString("reviewerID");
								reviewerStmt.setString(1, reviewerID);
								ResultSet reviewer = reviewerStmt.executeQuery();
								String classification = reviewer.getString("classification");
								if (classification.equals("R")) {
									rating += reviews.getDouble("rating");
									numReviews++;
								} else {
									if (originalRating < adjustedRating) {
										if (classification.equals("O")) {
											rating += reviews.getDouble("rating");
											numReviews++;
										} else if (classification.equals("P")) {
											rating += reviews.getDouble("rating") + alpha * (adjustedRating - originalRating);
											numReviews++;
										}
									} else {
										if (classification.equals("O")) {
											rating += reviews.getDouble("rating") + alpha * (adjustedRating - originalRating);
											numReviews++;
										} else if (classification.equals("P")) {
											rating += reviews.getDouble("rating");
											numReviews++;
										}
									}
								}
								reviewer.close();
							}
							reviews.close();
							String sql = "UPDATE items SET alpha" + column + " = ? WHERE asin = ?";
							PreparedStatement insertAlphas = itemConn.prepareStatement(sql);
							insertAlphas.setDouble(1, rating / numReviews);
							insertAlphas.setString(2, asin);
							insertAlphas.addBatch();
							insertAlphas.executeBatch();
							column++;
						}
					}
					System.out.println(x);
					x++;
				}
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void rankToRatingCorrelation() {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection itemConn = DriverManager.getConnection("jdbc:sqlite:" + itemPath);
				 PreparedStatement getItems = itemConn.prepareStatement("SELECT * FROM items");
				 ResultSet items = getItems.executeQuery();) {
				ArrayList<Double> inverseRanks = new ArrayList<>();
				ArrayList<Double> originalRatings = new ArrayList<>();
				ArrayList<Double> adjustedRatings = new ArrayList<>();
				ArrayList<Double> alpha0 = new ArrayList<>();
				ArrayList<Double> alpha1 = new ArrayList<>();
				ArrayList<Double> alpha2 = new ArrayList<>();
				ArrayList<Double> alpha3 = new ArrayList<>();
				ArrayList<Double> alpha4 = new ArrayList<>();
				ArrayList<Double> alpha5 = new ArrayList<>();
				ArrayList<Double> alpha6 = new ArrayList<>();
				ArrayList<Double> alpha7 = new ArrayList<>();
				ArrayList<Double> alpha8 = new ArrayList<>();
				ArrayList<Double> alpha9 = new ArrayList<>();
				ArrayList<Double> alpha10 = new ArrayList<>();
				while (items.next()) {
					if (items.getDouble(3) == NaN){
						originalRatings.add(0.0);
					}
					else {
						originalRatings.add(items.getDouble(3));
					}
					if (items.getDouble(4) == NaN) {
						adjustedRatings.add(0.0);
					}
					else {
						adjustedRatings.add(items.getDouble(4));
					}
					if (items.getDouble(5) == NaN) {
						alpha0.add(0.0);
					}
					else {
						alpha0.add(items.getDouble(5));
					}
					if (items.getDouble(6) == NaN) {
						alpha1.add(0.0);
					}
					else {
						alpha1.add(items.getDouble(6));
					}
					if (items.getDouble(7) == NaN) {
						alpha2.add(0.0);
					}
					else {
						alpha2.add(items.getDouble(7));
					}
					if (items.getDouble(8) == NaN) {
						alpha3.add(0.0);
					}
					else {
						alpha3.add(items.getDouble(8));
					}
					if (items.getDouble(9) == NaN) {
						alpha4.add(0.0);
					}
					else {
						alpha4.add(items.getDouble(9));
					}
					if (items.getDouble(10) == NaN) {
						alpha5.add(0.0);
					}
					else {
						alpha5.add(items.getDouble(10));
					}
					if (items.getDouble(11) == NaN) {
						alpha6.add(0.0);
					}
					else {
						alpha6.add(items.getDouble(11));
					}
					if (items.getDouble(12) == NaN) {
						alpha7.add(0.0);
					}
					else {
						alpha7.add(items.getDouble(12));
					}
					if (items.getDouble(13) == NaN) {
						alpha8.add(0.0);
					}
					else {
						alpha8.add(items.getDouble(13));
					}
					if (items.getDouble(14) == NaN) {
						alpha9.add(0.0);
					}
					else {
						alpha9.add(items.getDouble(14));
					}
					if (items.getDouble(15) == NaN) {
						alpha10.add(0.0);
					}
					else {
						alpha10.add(items.getDouble(15));
					}
					double temp = items.getDouble(2);
					if (items.wasNull()) {
						inverseRanks.add(0.0);
					}
					else {
						inverseRanks.add(1 / temp);
						//inverseRanks.add((101 - temp) / 100);
					}
				}
				System.out.println("Done compiling");
				double originalRatingCorrelation = calculateCorrelation(inverseRanks, originalRatings);
				double adjustedRatingCorrelation = calculateCorrelation(inverseRanks, adjustedRatings);
				double alpha0Correlation = calculateCorrelation(inverseRanks, alpha0);
				double alpha1Correlation = calculateCorrelation(inverseRanks, alpha1);
				double alpha2Correlation = calculateCorrelation(inverseRanks, alpha2);
				double alpha3Correlation = calculateCorrelation(inverseRanks, alpha3);
				double alpha4Correlation = calculateCorrelation(inverseRanks, alpha4);
				double alpha5Correlation = calculateCorrelation(inverseRanks, alpha5);
				double alpha6Correlation = calculateCorrelation(inverseRanks, alpha6);
				double alpha7Correlation = calculateCorrelation(inverseRanks, alpha7);
				double alpha8Correlation = calculateCorrelation(inverseRanks, alpha8);
				double alpha9Correlation = calculateCorrelation(inverseRanks, alpha9);
				double alpha10Correlation = calculateCorrelation(inverseRanks, alpha10);
				PreparedStatement insertCorrelations = itemConn.prepareStatement("INSERT INTO items (asin, originalRating, adjustedRating, alpha0, alpha1, alpha2, alpha3, alpha4, alpha5, alpha6, alpha7, alpha8, alpha9, alpha10) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)");
				insertCorrelations.setString(1, "Correlations");
				insertCorrelations.setDouble(2, originalRatingCorrelation);
				insertCorrelations.setDouble(3, adjustedRatingCorrelation);
				insertCorrelations.setDouble(4, alpha0Correlation);
				insertCorrelations.setDouble(5, alpha1Correlation);
				insertCorrelations.setDouble(6, alpha2Correlation);
				insertCorrelations.setDouble(7, alpha3Correlation);
				insertCorrelations.setDouble(8, alpha4Correlation);
				insertCorrelations.setDouble(9, alpha5Correlation);
				insertCorrelations.setDouble(10, alpha6Correlation);
				insertCorrelations.setDouble(11, alpha7Correlation);
				insertCorrelations.setDouble(12, alpha8Correlation);
				insertCorrelations.setDouble(13, alpha9Correlation);
				insertCorrelations.setDouble(14, alpha10Correlation);
				insertCorrelations.executeUpdate();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public double calculateCorrelation(ArrayList<Double> xs, ArrayList<Double> ys) {
		double sx = 0.0;
		double sy = 0.0;
		double sxx = 0.0;
		double syy = 0.0;
		double sxy = 0.0;

		int n = xs.size();

		for(int i = 0; i < n; ++i) {
			if (i == 1170) {
				int the = 0;
			}
			double x = xs.get(i);
			if (Double.isNaN(x)) {
				x = 0.0;
			}
			double y = ys.get(i);
			if (Double.isNaN(y)) {
				y = 0.0;
			}

			sx += x;
			sy += y;
			sxx += x * x;
			syy += y * y;
			sxy += x * y;
		}

		// covariation
		/*double cov = sxy / n - sx * sy / n / n;
		// standard error of x
		double sigmax = Math.sqrt(sxx / n -  sx * sx / n / n);
		// standard error of y
		double sigmay = Math.sqrt(syy / n -  sy * sy / n / n);

		// correlation is just a normalized covariation
		return cov / sigmax / sigmay;*/
		return (n * sxy - sx * sy)/(Math.sqrt((n * sxx - sx * sx) * (n * syy - sy * sy)));

	}
	public static void main (String[] args) {
		ItemCalc items = new ItemCalc("reviewers.db", "CDs_and_Vinyl_newEquation.db", "reviews.db");
		items.rankToRatingCorrelation();
	}
}
