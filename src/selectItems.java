import java.sql.*;

/**
 * Created by LFQLE on 2/24/2017.
 */
public class selectItems {

	public static void removeItemsNotInList(String listDBPath, String toBeRemovedPath, String newDBPath) {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection oldConn = DriverManager.getConnection("jdbc:sqlite:" + toBeRemovedPath);
				 PreparedStatement select = oldConn.prepareStatement("SELECT * FROM reviews");
				 ResultSet allReviews = select.executeQuery();
				 Connection newConn = DriverManager.getConnection("jdbc:sqlite:" + newDBPath);
				 PreparedStatement insert = newConn.prepareStatement("INSERT INTO reviews (reviewerID, asin, rating, title, content, helpfulVotes, totalVotes, label, pos, neg, neutral) values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)");) {
				String oldItem = "";
				int items = 0;
				while (items <= 3001 && allReviews.next()) {
					if(!allReviews.getString(2).equals(oldItem)) {
						items++;
						insert.executeBatch();
						System.out.println(items);
						oldItem = allReviews.getString(2);
					}
					insert.setString(1, allReviews.getString(1));
					insert.setString(2, allReviews.getString(2));
					insert.setInt(3, allReviews.getInt(3));
					insert.setString(4, allReviews.getString(4));
					insert.setString(5, allReviews.getString(5));
					insert.setInt(6, allReviews.getInt(6));
					insert.setInt(7, allReviews.getInt(7));
					insert.setString(8, allReviews.getString(8));
					insert.setDouble(9, allReviews.getDouble(9));
					insert.setDouble(10, allReviews.getDouble(10));
					insert.setDouble(11, allReviews.getDouble(11));
					insert.addBatch();
				}
				insert.executeBatch();

			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main (String[] args) {
		removeItemsNotInList("CDs_and_Vinyl.db", "reviews1.db", "reviews.db");
	}
}
