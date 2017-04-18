/**
 * Created by LFQLE on 2/15/2017.
 */

import java.sql.*;
import java.util.ArrayList;

public class DBHandler {

	public static ArrayList<String> getIDs(String databasePath) {
		ArrayList<String> itemIDs = new ArrayList<>();
		try {
			Class.forName("org.sqlite.JDBC");
			Connection conn = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT asin FROM items");
			int x = 1;
			while (rs.next()) {
				itemIDs.add(rs.getString("asin"));
				System.out.println("Got ID " + x);
				x++;
			}
			conn.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return itemIDs;
	}

	public static void writeRank(String databasePath, ArrayList<ArrayList<Object>> ranks) {
		try {
			Class.forName("org.sqlite.JDBC");
			Connection conn = DriverManager.getConnection("jdbc:sqlite:" + databasePath);
			PreparedStatement insertRank = conn.prepareStatement("UPDATE items SET rank1 = ?1, rank2 = ?2, rank3 = ?3 WHERE asin = ?4;");
			int x = 1;
			for (ArrayList<Object> rank: ranks) {
				insertRank.setInt(1, (int) rank.get(0));
				insertRank.setInt(2, (int) rank.get(1));
				insertRank.setInt(3, (int) rank.get(2));
				insertRank.setString(4, (String) rank.get(3));
				insertRank.addBatch();
				if (x % 100 == 0) {
					conn.setAutoCommit(false);
					insertRank.executeBatch();
					insertRank.clearParameters();
					insertRank.clearBatch();
				}
				x++;
			}
			conn.setAutoCommit(false);
			insertRank.executeBatch();
			insertRank.clearParameters();
			insertRank.clearBatch();
			conn.commit();
			insertRank.close();
			conn.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
}
