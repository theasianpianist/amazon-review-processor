/**
 * Created by LFQLE on 2/15/2017.
 */
import org.jsoup.*;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class getRank {
	public getRank(String ID) {
		itemID = ID;
	}
	public org.jsoup.nodes.Document jsoup_load_with_retry(String url) throws IOException {
		int max_retry = 10;
		int retry = 1;
		int sleep_sec = 3;
		org.jsoup.nodes.Document content = null;

		while(retry <= max_retry){
			try {
				content = Jsoup.connect(url).timeout(10 * 1000).header("Connection", "keep-alive").userAgent("Mozilla/17.0").post();
				break;
			} catch (Exception ex){
				//wait before retry
				System.out.println(ex.getMessage() + " retrying..");
				try {
					TimeUnit.SECONDS.sleep(sleep_sec);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			retry++;
		}
		return content;
	}

	public org.jsoup.nodes.Document getPage() {
		String url = "http://www.amazon.com/dp/" + itemID;
		org.jsoup.nodes.Document itemPage = null;
		try {
			// collect review from each of the review pages;
			boolean captcha = true;
			while (captcha) {
				itemPage = jsoup_load_with_retry(url);
				//System.out.println("Bot check");
				if (!itemPage.select("title[dir=ltr]").isEmpty()) {
					System.out.print("\r");
					System.out.print("Testing Page ");
					System.out.print(" Bot redirect");
				}
				else {
					System.out.println("");
					System.out.println("Success");
					captcha = false;
				}
			}

		}
		catch (Exception e) {
			System.out.println(itemID + " " + "Exception" + " " + e.toString());
		}
		return itemPage;
	}

	public ArrayList<Object> findRank() {
		org.jsoup.nodes.Document itemPage = getPage();
		ArrayList<Object> rankings = new ArrayList<>();
		Elements detailTable = itemPage.select("table#productDetails_detailBullets_sections1 tr");
		Element rankingRow = detailTable.last();
		for (Element row: detailTable) {
			if (row.text().contains("Best Sellers Rank")) {
				rankingRow = row;
				break;
			}
		}
		try {
			Elements rawRankings = rankingRow.select("span span");
			for (Element ranking : rawRankings) {
				String text = ranking.text();
				text = text.substring(0, getFirstSpace(text));
				text = text.replace("#", "");
				text = text.replace(",", "");
				rankings.add(Integer.parseInt(text));
			}
			while (rankings.size() < 3) {
				rankings.add(0);
			}
			rankings.add(itemID);
		}
		catch(Exception e) {
			Elements rawRankings = itemPage.select(".zg_hrsr_rank");
			for (Element ranking : rawRankings) {
				String text = ranking.text();
				text = text.replace("#", "");
				text = text.replace(",", "");
				rankings.add(Integer.parseInt(text));
			}
			while (rankings.size() < 3) {
				rankings.add(0);
			}
			rankings.add(itemID);

		}
		return rankings;
	}

	public int getFirstSpace(String string) { //returns the index position of the first space in a string
		int firstSpace = -1;
		int length = string.length();
		for (int i = 0; i < length; i++) {
			if (string.substring(i, i + 1).equals(" ")) {
				firstSpace = i;
				i = length;
			}
		}
		return firstSpace;
	}
	String itemID;
}
