import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.json.JSONObject;

import java.sql.*;

/**
 * Created by LFQLE on 2/20/2017.
 */
public class Sentiment {
	public static int requestsMade = 0;
	public static void getSentiment(String path) {
		try {
			Class.forName("org.sqlite.JDBC");
			try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + path);
				 PreparedStatement getEntries = conn.prepareStatement("SELECT * FROM reviews");
				 PreparedStatement insertScores = conn.prepareStatement("UPDATE reviews SET label = ?, pos = ?, neg = ?, neutral = ? WHERE reviewerID = ? and asin = ?");
				 ResultSet entries = getEntries.executeQuery()) {
				int x = 1;
				while (entries.next() && requestsMade <= 2500000) {
					String content = entries.getString("content");
					Double test = entries.getDouble("label");
					if (test == 0) {
						JSONObject label = getScores(content);
						JSONObject scores = (JSONObject) label.get("probability");
						insertScores.setString(1, (String) label.get("label"));
						insertScores.setDouble(2, (double) scores.get("pos"));
						insertScores.setDouble(3, (double) scores.get("neg"));
						insertScores.setDouble(4, (double) scores.get("neutral"));
						insertScores.setString(5, entries.getString("reviewerID"));
						insertScores.setString(6, entries.getString("asin"));
						insertScores.addBatch();
					}
					else {
						System.out.println("Duplicate found! " + x);
					}
					if (x % 100 == 0) {
						insertScores.executeBatch();
						System.out.println(x + " scores inserted");
					}
					x++;
				}
				insertScores.executeBatch();
				System.out.println(x -1  + " scores inserted");
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static JSONObject getScores(String content) {
		JSONObject toReturn = new JSONObject();
		try {
			HttpResponse<JsonNode> response = Unirest.post("https://japerk-text-processing.p.mashape.com/sentiment/")
					.header("X-Mashape-Key", "k8ohHmKpPDmshRZexNYIrgJ9g9mop1xn7Wfjsn5fkaHEs5JCbk")
					.header("Content-Type", "application/x-www-form-urlencoded")
					.header("Accept", "application/json")
					.field("language", "english")
					.field("text", content)
					.asJson();
			requestsMade++;
			toReturn = (JSONObject) new JSONObject(response).get("body");
			toReturn = (JSONObject) toReturn.get("object");
		} catch (UnirestException e) {
			e.printStackTrace();
		}
		return toReturn;
	}

	public static void main(String[] args) {
		getSentiment("reviews.db");
	}
}
