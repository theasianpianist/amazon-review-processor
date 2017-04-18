import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.spec.ECField;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;

import org.json.*;
import java.sql.*;

/**
 * Created by LFQLE on 2/16/2017.
 */
public class createItemDB {


	public static void rawToDB(String path) {
		String itemList = "items.db";
		ArrayList<Object[]> reviews = new ArrayList<>();
		ArrayList<Reviewer> reviewers = new ArrayList<>();
		ArrayList<String> reviewerIDs = new ArrayList<>();
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(path);
			br = new BufferedReader(fr);
			String sCurrentLine;
			br = new BufferedReader(new FileReader(path));
			int x = 1;
			Object[] currentReview;
			while ((sCurrentLine = br.readLine()) != null) {
				try {
					Class.forName("org.sqlite.JDBC");
					try (Connection itemListConn = DriverManager.getConnection("jdbc:sqlite:" + itemList);
						 PreparedStatement getItems = itemListConn.prepareStatement("SELECT * FROM items WHERE asin = ?")) {
						currentReview = reviewParser(sCurrentLine);
						getItems.setString(1, (String) currentReview[1]);
						ResultSet items = getItems.executeQuery();
						if (items.next()) { //only processes review if asin is contained within item list
							if (!reviewerIDs.contains(currentReview[0])) { //If reviewer list does not contain customer of current review, add to reviewer list
								reviewerIDs.add((String) currentReview[0]);
								reviewers.add(new Reviewer((String) currentReview[0], currentReview));
							} else { //If reviewer list does contain customer of current review, add review to customer entry
								int index = reviewerIDs.indexOf((String) currentReview[0]);
								String temp = reviewers.get(index).getReviewerID();
								if (temp.equals(currentReview[0])) {
									reviewers.get(index).addReview(currentReview);
								}
							}

							if (x > 0) {
								reviews.add(currentReview);
							}
						}
					}
				}
				catch (Exception e) {
					e.printStackTrace();
				}
				//System.out.println("Review " + x);
				if (x > 0 && x % 10000 == 0) {
					reviewsToDB(reviews, "reviews");
					System.out.println(x + " reviews added");
					reviews.clear();
				}
				x++;
			}
			reviewsToDB(reviews, "reviews");
			System.out.println(x + " reviews added");
			reviews.clear();
			reviewersToDB(reviewers, "reviews");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (fr != null)
					fr.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	public static Object[] reviewParser(String data) {
		JSONObject obj = new JSONObject(data);
		Object[] review = new Object[7];
		String[] fields = {"reviewerID", "asin", "overall", "summary", "reviewText", "helpful"};
		int x = 0;
		for (String key : fields) {
			review[x] = obj.get(key);
			x++;
		}
		String votes = review[5].toString();
		votes = votes.substring(1, votes.length() - 1);
		String[] temp = votes.split(",");
		temp[0] = temp[0].replace(",","");
		temp[1] = temp[1].replace(",","");
		review[5] = temp[0];
		review[6] = temp[1];
		return review;
	}

	public static void reviewsToDB(ArrayList<Object[]> reviews, String database) {
		try {
			Class.forName("org.sqlite.JDBC");
			for (int num = 1; num <= 4; num++) {
				if (!(new File(database + ".db").isFile())) {
					Statement stmt = null;
					try {
						Connection conn = DriverManager
								.getConnection("jdbc:sqlite:" + database + ".db");
						stmt = conn.createStatement();
						String sql = "CREATE TABLE reviews ( " +
								"reviewerID TEXT, " +
								"asin TEXT, " +
								"rating NUMERIC, " +
								"title TEXT, " +
								"content TEXT, " +
								"helpfulVotes NUMERIC, " +
								"totalVotes NUMERIC);";
						stmt.executeUpdate(sql);
						stmt.close();
						conn.close();
						System.out.println("Reviews created successfully");
					} catch (Exception e) {
						System.err.println(e.getClass().getName() + ": "
								+ e.getMessage());
						System.exit(0);
					}
				}
			}
			if (!(new File("items.db").isFile())) {
				Statement stmt = null;
				try {
					Connection conn = DriverManager.getConnection("jdbc:sqlite:" + "items.db");
					stmt = conn.createStatement();
					String sql = "CREATE TABLE IF NOT EXISTS items( " +
							"asin TEXT, " +
							"rank1 NUMERIC, " +
							"rank2 NUMERIC, " +
							"rank3 NUMERIC);";
					stmt.executeUpdate(sql);
					stmt.close();
					conn.close();
					System.out.println("Items created successfully");
				}
				catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}
			}
			String asin;
			int x = 1;
			String path = database + ".db";
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + path);
				 PreparedStatement insertReview = conn.prepareStatement("insert into reviews (" +
						 "reviewerID, " +
						 "asin, " +
						 "rating, " +
						 "title, " +
						 "content, " +
						 "helpfulVotes, " +
						 "totalVotes) values (?1, ?2, ?3, ?4, ?5, ?6, ?7);");
				 Connection conn2 = DriverManager.getConnection("jdbc:sqlite:"
						 + "items.db");
				 PreparedStatement insertItem = conn2.prepareStatement("insert into items (" +
						 "asin, " +
						 "rank1, " +
						 "rank2, " +
						 "rank3) values (?1, ?2, ?3, ?4);");) {
				Class.forName("org.sqlite.JDBC");
				for (Object[] review : reviews) {
					asin = (String) review[1];
					String sql = "select * from items where asin = ? ";
					PreparedStatement pstmt = conn2.prepareStatement(sql);
					pstmt.setString(1, asin);
					ResultSet rs = pstmt.executeQuery();
					Boolean exists = rs.next();
					if (!exists) {
						insertItem.setString(1, asin);
						insertItem.setInt(2, 0);
						insertItem.setInt(3, 0);
						insertItem.setInt(4, 0);
						insertItem.addBatch();
						conn2.setAutoCommit(false);
						insertItem.executeBatch();
						conn2.commit();
						//insertItem.clearParameters();
					}
//					insertReview.setString(1, (String) review[0]);
//					insertReview.setString(2, (String) review[1]);
//					insertReview.setDouble(3, (Double) review[2]);
//					insertReview.setString(4, (String) review[3]);
//					insertReview.setString(5, (String) review[4]);
//					insertReview.setInt(6, Integer.parseInt((String) review[5]));
//					insertReview.setInt(7, Integer.parseInt((String) review[6]));
//					insertReview.addBatch();
					//System.out.println("Processed review " + x + " of " + reviews.size() + " in " + num);
					x++;
				}

				conn.setAutoCommit(false);

				//insertReview.executeBatch();
				conn.commit();
			}
			catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}

		catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void reviewersToDB(ArrayList<Reviewer> reviewers, String path) {
		try {
			Class.forName("org.sqlite.JDBC");
			if (!(new File("reviewers.db").isFile())) {
				Statement stmt = null;
				try {
					Connection conn = DriverManager
							.getConnection("jdbc:sqlite:" + "reviewers.db");
					stmt = conn.createStatement();
					String sql = "CREATE TABLE reviewers ( " +
							"reviewerID TEXT, " +
							"numReviews NUMERIC, " +
							"avgRating NUMERIC, " +
							"helpfulVotes NUMERIC, " +
							"totalVotes NUMERIC);";
					stmt.executeUpdate(sql);
					stmt.close();
					conn.close();
					System.out.println("Reviews created successfully");
				} catch (Exception e) {
					System.err.println(e.getClass().getName() + ": "
							+ e.getMessage());
					System.exit(0);
				}
			}
			Connection conn = DriverManager.getConnection("jdbc:sqlite:"
					+ "reviewers.db");
			PreparedStatement insertReviewer = conn.prepareStatement("insert into reviewers (" +
					"reviewerID, " +
					"numReviews, " +
					"avgRating, " +
					"helpfulVotes," +
					"totalVotes) values (?1, ?2, ?3, ?4, ?5);");
			int x = 1;
			for (Reviewer reviewer : reviewers) {
				insertReviewer.setString(1, reviewer.getReviewerID());
				insertReviewer.setInt(2, reviewer.numReviews());
				insertReviewer.setDouble(3, reviewer.avgRating());
				insertReviewer.setInt(4, reviewer.helpfulVotes());
				insertReviewer.setInt(5, reviewer.totalVotes());
				insertReviewer.addBatch();
				if (x % 1000 == 0) {
					conn.setAutoCommit(false);
					insertReviewer.executeBatch();
					insertReviewer.clearParameters();
				}
				System.out.println("Processed " + x + " reviewer out of " + reviewers.size());
				x++;
			}

			conn.setAutoCommit(false);
			insertReviewer.executeBatch();
			insertReviewer.clearParameters();
			conn.commit();
			insertReviewer.close();
			conn.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}
	public static void addCols(String path) {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + path);
				 PreparedStatement addLabel = conn.prepareStatement("ALTER TABLE reviews ADD COLUMN label TEXT");
				 PreparedStatement addPos = conn.prepareStatement("ALTER TABLE reviews ADD COLUMN pos NUMERIC");
				 PreparedStatement addNeg = conn.prepareStatement("ALTER TABLE reviews ADD COLUMN neg NUMERIC");
				 PreparedStatement addNeutral = conn.prepareStatement("ALTER TABLE reviews ADD COLUMN neutral NUMERIC")){
				addLabel.executeUpdate();
				addPos.executeUpdate();
				addNeg.executeUpdate();
				addNeutral.executeUpdate();
				System.out.println("Update " + path);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void mergeDBs(String path) {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + "reviews1.db")) {
				PreparedStatement setPragma = conn.prepareStatement("PRAGMA journal_mode = TRUNCATE");
				setPragma.executeQuery();
				setPragma.close();
				for (int i = 2; i < 999; i++) {
					String database = "reviews" + i + ".db";
					String sql = "ATTACH '" + database + "' AS toMerge";
					System.out.println(sql);
					try (PreparedStatement attach = conn.prepareStatement(sql)) {
						attach.executeUpdate();
						PreparedStatement merge = conn.prepareStatement("INSERT INTO reviews SELECT * FROM toMerge.reviews");
						merge.executeUpdate();
						PreparedStatement detach = conn.prepareStatement("DETACH toMerge");
						detach.executeUpdate();
						merge.close();
						detach.close();
					}
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		rawToDB("D:\\Documents\\Code\\Data\\Electronics_5.json");
		mergeDBs("reviews1.db");
		for (int i = 1; i <= 999; i++) {
			addCols("reviews" + i + ".db");
		}
		Dedup reviewers = new Dedup("reviews\\reviews2.db");
		reviewers.deduplicate();
		ReviewerCalc calc = new ReviewerCalc("reviewers.db", "items.db", "reviews.db");
		calc.calculate();
	}
}
