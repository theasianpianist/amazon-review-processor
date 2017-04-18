/**
 * Created by LFQLE on 2/18/2017.
 */

import java.sql.*;
public class Dedup {

	private String path;

	public Dedup(String databasePath) {
		path = databasePath;
	}

	public void deduplicate() {
		String reviewerID;
		String asin;
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + path);
				 PreparedStatement deleteEntry = conn.prepareStatement("DELETE FROM reviews WHERE rowid NOT IN (SELECT max(rowid) FROM reviews GROUP BY reviewerID, asin)");) {
				deleteEntry.executeUpdate();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main (String[] args) {
		Dedup database = new Dedup("reviews.db");
		database.deduplicate();
	}
}
