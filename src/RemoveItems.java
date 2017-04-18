import java.io.File;
import java.sql.*;

/**
 * Created by LFQLE on 2/21/2017.
 */
public class RemoveItems {

	public static void removeItemsNotInList(String listDBPath, String toBeRemovedPath) {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection listConn = DriverManager.getConnection("jdbc:sqlite:" + listDBPath);
				 PreparedStatement getFromList = listConn.prepareStatement("SELECT * FROM items WHERE asin = ?");
				 Connection tbrConn = DriverManager.getConnection("jdbc:sqlite:"
						 + toBeRemovedPath);
				 PreparedStatement getAsinTbr = tbrConn.prepareStatement("SELECT * FROM reviews");
				 PreparedStatement deleteAsin = tbrConn.prepareStatement("DELETE FROM reviews WHERE asin = ?");
				 ResultSet asinList = getFromList.executeQuery();
				 ResultSet tbrList = getAsinTbr.executeQuery();) {
				int x = 0;
				tbrConn.setAutoCommit(false);
				while(tbrList.next()) {
					String asin = tbrList.getString("asin");
					getFromList.setString(1, asin);
					ResultSet rs = getFromList.executeQuery();
					Boolean exists = rs.next();
					if (!exists) {
						deleteAsin.setString(1, asin);
						deleteAsin.executeUpdate();
						tbrConn.commit();
						x++;
						System.out.println("Removed " + x + " asins");
					}
				}
				deleteAsin.executeBatch();
				System.out.println("Removed " + x + " asins");
				//insertReview.executeBatch();
				tbrConn.commit();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main (String[] args) {
		removeItemsNotInList("Z:\\CDs_and_Vinyl.db", "Z:\\reviews1.db");
	}
}
