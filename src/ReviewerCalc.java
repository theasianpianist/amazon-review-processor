import java.sql.*;
import java.util.ArrayList;

import static java.lang.Double.NaN;

/**
 * Created by LFQLE on 2/18/2017.
 */
public class ReviewerCalc {
	String reviewerPath;
	String itemPath;
	String reviewPath;
	public ReviewerCalc(String reviewerDBPath, String itemDBPath, String reviewDBPath) {
		reviewerPath = reviewerDBPath;
		itemPath = itemDBPath;
		reviewPath = reviewDBPath;
	}

	public void calculate() {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection reviewerConn = DriverManager.getConnection("jdbc:sqlite:" + reviewerPath);
				 PreparedStatement selectAll = reviewerConn.prepareStatement("SELECT reviewerID FROM reviewers");
				 PreparedStatement insertCalcs = reviewerConn.prepareStatement("UPDATE reviewers SET avgRating = ?, stddev = ?, correlation = ? WHERE reviewerID = ?");
				 ResultSet reviewers = selectAll.executeQuery();
				 Connection itemConn = DriverManager.getConnection("jdbc:sqlite:" + itemPath);
				 PreparedStatement getRank = itemConn.prepareStatement("SELECT rank1 FROM items WHERE asin = ?");
				 Connection reviewConn = DriverManager.getConnection("jdbc:sqlite:" + reviewPath);
				 PreparedStatement getReviews = reviewConn.prepareStatement("SELECT asin, rating FROM reviews WHERE reviewerID = ?")) {
				int x = 1;
				while (reviewers.next()) {
					String reviewerID;
					ArrayList<String> asins = new ArrayList<>();
					ArrayList<Double> ratings = new ArrayList<>();
					ArrayList<Double> inverseRank = new ArrayList<>();
					reviewerID = reviewers.getString("reviewerID");
					getReviews.setString(1, reviewerID);
					ResultSet reviews = getReviews.executeQuery();
					while (reviews.next()) {
						asins.add(reviews.getString("asin"));
						ratings.add(reviews.getDouble("rating"));
					}
					reviews.close();
					double lowestRank = 0;
					for (String asin : asins) { //finds appropriate inverse rank for all items
						getRank.setString(1, asin);
						ResultSet ranks = getRank.executeQuery();
						lowestRank = ranks.getDouble("rank1");
						if (ranks.wasNull()) {
							inverseRank.add(0.0);
						}
						else {
							inverseRank.add((double) (1 / lowestRank));
						}
						ranks.close();
					}
					double mean = calculateMean(ratings);
					double stdDev = calculateStdDev(ratings);
					double correlation = calculateCorrelation(ratings, inverseRank);
					insertCalcs.setDouble(1, mean);
					insertCalcs.setDouble(2, stdDev);
					insertCalcs.setDouble(3, correlation);
					insertCalcs.setString(4, reviewerID);
					insertCalcs.addBatch();
					if (x % 100 == 0) {
						insertCalcs.executeBatch();
						System.out.println(x + " reviewers updated");
					}
					x++;

				}
				insertCalcs.executeBatch();
				System.out.println(x + " reviewers updated");
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void calculateCorrelation() {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection reviewConn = DriverManager.getConnection("jdbc:sqlite:" + reviewPath);
				 Connection reviewerConn = DriverManager.getConnection("jdbc:sqlite:" + reviewerPath);
				 Connection itemConn = DriverManager.getConnection("jdbc:sqlite:" + itemPath);
				 PreparedStatement getReviewers = reviewerConn.prepareStatement("SELECT * FROM reviewers");
				 PreparedStatement getReviews = reviewConn.prepareStatement("SELECT asin, rating WHERE reviewerID = ?");
				 PreparedStatement getItem = reviewConn.prepareStatement("SELECT originalRating, adjustedRating WHERE asin = ?");
				 ResultSet reviewers = getReviewers.executeQuery();) {
				while (reviewers.next()) {
					double originalRating = 0;
					double adjustedRating = 0;
					double numReviews = 0;
					String classification = reviewers.getString("classification");
					String reviewerID = reviewers.getString("reviewerID");
					//getReviews.setString(1, reviewerID);
					ResultSet reviews = getReviews.executeQuery();
					while (reviews.next()) {
						//getItem.setString(1, reviews.getString("asin"));
						ResultSet item =  getItem.executeQuery();
						originalRating += item.getDouble("originalRating");
						if (classification.equals("R")) {
							adjustedRating += reviews.getDouble("rating");
							numReviews++;
						} else {
							if (originalRating < adjustedRating) {
								if (classification.equals("O")) {
									adjustedRating += reviews.getDouble("rating");
									numReviews++;
								} else if (classification.equals("P")) {
									//adjustedRating += reviews.getDouble("rating") + alpha * (adjustedRating - originalRating);
									numReviews++;
								}
							} else {
								if (classification.equals("O")) {
									//adjustedRating += reviews.getDouble("rating") + alpha * (adjustedRating - originalRating);
									numReviews++;
								} else if (classification.equals("P")) {
									adjustedRating += reviews.getDouble("rating");
									numReviews++;
								}
							}
						}

					}
					
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void addCols () {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection reviewerConn = DriverManager.getConnection("jdbc:sqlite:" + reviewerPath);
				 PreparedStatement addAlpha0 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha0 NUMERIC");
				 PreparedStatement addAlpha1 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha1 NUMERIC");
				 PreparedStatement addAlpha2 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha2 NUMERIC");
				 PreparedStatement addAlpha3 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha3 NUMERIC");
				 PreparedStatement addAlpha4 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha4 NUMERIC");
				 PreparedStatement addAlpha5 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha5 NUMERIC");
				 PreparedStatement addAlpha6 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha6 NUMERIC");
				 PreparedStatement addAlpha7 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha7 NUMERIC");
				 PreparedStatement addAlpha8 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha8 NUMERIC");
				 PreparedStatement addAlpha9 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha9 NUMERIC");
				 PreparedStatement addAlpha10 = reviewerConn.prepareStatement("ALTER TABLE reviewers ADD COLUMN alpha10  NUMERIC")) {
				addAlpha0.executeUpdate();
				addAlpha1.executeUpdate();
				addAlpha2.executeUpdate();
				addAlpha3.executeUpdate();
				addAlpha4.executeUpdate();
				addAlpha5.executeUpdate();
				addAlpha6.executeUpdate();
				addAlpha7.executeUpdate();
				addAlpha8.executeUpdate();
				addAlpha9.executeUpdate();
				addAlpha10.executeUpdate();
				System.out.println("Added columns to " + reviewerPath);
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void calculateReliability () {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + reviewerPath);
				 PreparedStatement getValues = conn.prepareStatement("SELECT reviewerID, avgRating, stddev, correlation FROM reviewers");
				 ResultSet reviewers = getValues.executeQuery();
				 PreparedStatement insertReliability = conn.prepareStatement("UPDATE reviewers SET classification = ? WHERE reviewerID = ?")) {
				int x = 0;
				while (reviewers.next()) {
					double average = reviewers.getDouble("avgRating");
					double stdDev = reviewers.getDouble("stddev");
					double correlation = reviewers.getDouble("Correlation");
					double dist1 = calculateEucDist(average, 5, stdDev, 0, correlation, 0);
					double dist2 = calculateEucDist(average, 1, stdDev, 0, correlation, 0);
					double dist3 = calculateEucDist(average, 3, stdDev, 2, correlation, 1);
					double dist4 = calculateEucDist(average, 3, stdDev, 2, correlation, -1);
					double smallestDist = Math.min(dist1, Math.min(dist2, Math.min(dist3, dist4)));
					if (smallestDist == dist1) {
						insertReliability.setString(1, "O");
					}
					else if (smallestDist == dist2) {
						insertReliability.setString(1, "P");
					}
					else if (smallestDist == dist4) {
						insertReliability.setString(1, "U");
					}
					else {
						insertReliability.setString(1, "R");
					}
					insertReliability.setString(2, reviewers.getString("reviewerID"));
					insertReliability.addBatch();
					if (x % 1000 == 0 && x != 0) {
						insertReliability.executeBatch();
						System.out.println("Calculated reliability for " + x + " reviewers");
					}
					x++;
				}
				insertReliability.executeBatch();
				System.out.println("Calculated reliability for " + x + " reviewers");
			}

		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public double calculateEucDist(double x1, double y1, double x2, double y2, double x3, double y3) {
		double firstTerm = Math.pow((x1 - y1) / 4, 2);
		double secondTerm = Math.pow((x2 - y2) / 2, 2);
		double thirdTerm = Math.pow((x3 - y3) / 2, 2);
		return Math.sqrt(firstTerm + secondTerm + thirdTerm);
	}

	public double calculateStdDev(ArrayList<Double> ratings) {
		double mean = calculateMean(ratings);
		ArrayList<Double> subMean = new ArrayList<>();
		for (Double num : ratings) {
			subMean.add(Math.pow(num - mean, 2));
		}
		return calculateMean(subMean);
	}

	public double calculateCorrelation(ArrayList<Double> xs, ArrayList<Double> ys) {
		double sx = 0.0;
		double sy = 0.0;
		double sxx = 0.0;
		double syy = 0.0;
		double sxy = 0.0;

		int n = xs.size();

		for(int i = 0; i < n; ++i) {
			double x = xs.get(i);
			double y = ys.get(i);

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

	public double calculateMean(ArrayList<Double> values) {
		double total = 0;
		for (Double value : values) {
			total += value;
		}
		return total / values.size();
	}

	public static void main (String[] args) {
		ReviewerCalc name = new ReviewerCalc("reviewers.db", "CDs_and_Vinyl.db", "reviews.db");
		name.calculateReliability();
	}
}
