/**
 * Created by LFQLE on 2/18/2017.
 */
import java.util.ArrayList;

public class getItemRanks {
	public static void main (String[] args) {
		String path = "items.db";
		ArrayList<String> itemIDs = DBHandler.getIDs(path);
		ArrayList<ArrayList<Object>> ranks = new ArrayList<>();
		int x = 1;
		for (String id : itemIDs) {
			ranks.add(new getRank(id).findRank());
			if (x % 1000 == 0) {
				DBHandler.writeRank(path, ranks);
				ranks.clear();
				System.out.println(x + " ranks written");
			}
			x++;
		}
		DBHandler.writeRank(path, ranks);
	}
}
