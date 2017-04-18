import java.io.File;
import java.lang.reflect.Array;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by LFQLE on 2/16/2017.
 */
public class dbWriter extends Thread {
	private Thread t;
	//private ArrayList<Object[]> reviews = new ArrayList<>();
	private List<Object[]> reviews;
	private int num;
	private String database;

	public dbWriter(List<Object[]> reviewsToWrite, String databaseName, int fileNum) {
		this.num = fileNum;
		this.database = databaseName;
		this.reviews = reviewsToWrite;
	}

	public void run() {
		String path = database + num + ".db";
		try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + path);
			 PreparedStatement insertReview = conn.prepareStatement("insert into reviews (" +
					 "reviewerID, " +
					 "asin, " +
					 "rating, " +
					 "title, " +
					 "content, " +
					 "helpfulVotes, " +
					 "totalVotes) values (?1, ?2, ?3, ?4, ?5, ?6, ?7);");) {
			System.out.println("Writing from thread " + num);
			Class.forName("org.sqlite.JDBC");
			int x = 1;
			for (Object[] review : reviews) {
				insertReview.setString(1, (String) review[0]);
				insertReview.setString(2, (String) review[1]);
				insertReview.setDouble(3, (Double) review[2]);
				insertReview.setString(4, (String) review[3]);
				insertReview.setString(5, (String) review[4]);
				insertReview.setInt(6, Integer.parseInt((String) review[5]));
				insertReview.setInt(7, Integer.parseInt((String) review[6]));
				insertReview.addBatch();
				//System.out.println("Processed review " + x + " of " + reviews.size() + " in " + num);
				x++;
			}

			conn.setAutoCommit(false);
			System.out.println("Executing batch in " + num);
			insertReview.executeBatch();
			insertReview.close();
			conn.commit();
			conn.close();
		}
		catch (Exception e) {
			System.out.println("Thread" + num);
			e.printStackTrace();
			System.exit(1);
		}
	}

	public void start () {
		if (t == null) {
			t = new Thread (this);
			t.start ();
		}
	}
}
