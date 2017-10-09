package utilClasses;

import java.util.Arrays;
import java.util.List;

public class DataParser {

    private String line;
    private List<String> words;

    private final int CITY_ID = 7;
    private final int REGION_ID = 6;
    private final int MAX_CITY_INDEX = 395;

    private final int MIN_BIDDING_PRICE = 250;
    private final int BIDDING_PRICE_INDEX = 19;

    private final int NORMAL_SIZE = 16;

    public DataParser(String line) {
        this.line = line;
        words = Arrays.asList(line.split("\t"));
    }

    public int getCityID() {
        return Integer.parseInt(words.get(CITY_ID));
    }

    public int getRegionID() {
        return Integer.parseInt(words.get(REGION_ID));
    }

    public boolean isValidForMapping() {
        return words.size() == 26 && getCityID() <= MAX_CITY_INDEX && getBiddingPrice() > MIN_BIDDING_PRICE;
    }

    public int getBiddingPrice() {
        return Integer.parseInt(words.get(BIDDING_PRICE_INDEX));
    }

    public List<String> getWords() {
        return words;
    }
}
