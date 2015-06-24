package com.eyeview.webscraper.scrapers;

import au.com.bytecode.opencsv.CSVReader;
import com.eyeview.utils.aws.S3Manager;
import com.eyeview.webscraper.scrape.multiscraper.BaseMultiScraper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * User: vash
 * Date: 3/13/15
 * Time: 12:20 PM
 */
public class PGCVS extends BaseMultiScraper {

    //INPUT FEED - CHANGE THIS FOR EVERY SCRAPER
    private static String INPUT_FEED = "s3://eyeview-prod-assets/CLIENT_DATA/cvs/cvs_zip_list.csv";

    //member variables
    private static String INPUT_ZIP = "ev_input_zip_ev";
    private static String STORE_ID = "ev_store_id_ev";
    private static String STORE_NAME = "ev_store_name_ev";
    private static String ADDRESS = "ev_address_ev";
    private static String CITY = "ev_city_ev";
    private static String STATE = "ev_state_ev";
    private static String ZIP = "ev_zip_ev";
    private static String LAT = "ev_lat_ev";
    private static String LONG = "ev_long_ev";
    private static String TITLE1 = "ev_title1_ev";
    private static String IMAGE1 = "ev_image1_ev";
    private static String PRICE1 = "ev_price1_ev";
    private static String SCENESELECTOR1 = "ev_sceneselector1_ev";
    private static String DOLLAR1 = "ev_dollar1_ev";
    private static String CENT1 = "ev_cent1_ev";
    private static String FINEPRINT1 = "ev_fineprint1_ev";
    private static String DESCRIPTION1 = "ev_description1_ev";
    private static String TITLE2 = "ev_title2_ev";
    private static String IMAGE2 = "ev_image2_ev";
    private static String PRICE2 = "ev_price2_ev";
    private static String SCENESELECTOR2 = "ev_sceneselector2_ev";
    private static String DOLLAR2 = "ev_dollar2_ev";
    private static String CENT2 = "ev_cent2_ev";
    private static String FINEPRINT2 = "ev_fineprint2_ev";
    private static String DESCRIPTION2 = "ev_description2_ev";
    private static String INPUT = "ev_input_ev";

    //APIs
    private static String API_1 = "http://api.shoplocal.com/cvs/2012.2/xml/getstores.aspx?campaignid=1be5c664181ed971&citystatezip=";
    private static String API_2 = "http://api.shoplocal.com/cvs/2012.2/xml/getretailertaglistings.aspx?campaignid=1be5c664181ed971" +
            "&resultset=full&listingimagewidth=200&retailertagid=2806&sortby=10&pd=46BBB2453" +
            "F1DEEA8DDA3883648CDE154CABFA190956908C5E313294C45947DD95354857B8DE006EC4ECFF8C860" +
            "74&storeid=";

    @Override
    public long getDvcProjectId() {
        return 15950l;
    }


    @Override
    public void configureScraper() {
        disableJavaScript();
        addCollectField(INPUT_ZIP, STORE_ID, STORE_NAME, ADDRESS, CITY, STATE, ZIP, LAT, LONG, TITLE1, IMAGE1, PRICE1, SCENESELECTOR1, DOLLAR1, CENT1, FINEPRINT1, DESCRIPTION1, TITLE2, IMAGE2, PRICE2, SCENESELECTOR2, DOLLAR2, CENT2, FINEPRINT2, DESCRIPTION2, INPUT);
    }

/*    public List<Map<String, String>> getMultiplier() {
        return Lists.<Map<String, String>>newArrayList(ImmutableMap.<String, String>of(
                INPUT_ZIP, "07607"
        ));
    }*/

    @Override
    public List<Map<String, String>> getMultiplier() {
        List<Map<String,String>> rowList = new LinkedList<>();

        try {
            File csvFile = S3Manager.getInstance().getFileObject(INPUT_FEED);
            CSVReader csvReader = new CSVReader(new FileReader(csvFile));
            String [] nextLine;

            while( (nextLine = csvReader.readNext()) != null ){
                Map<String, String> aRow = Maps.newHashMap();
                aRow.put(INPUT_ZIP, nextLine[0]);
                rowList.add(aRow);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to load baseline CSV");
        }

        return rowList;
    }

    protected void scrapeImpl(Map<String, String> requestData){
        //write columns we have
        String input_zip= requestData.get(INPUT_ZIP);

        Document storeDocument;
        try {
            storeDocument = Jsoup.connect(API_1 + input_zip).get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Elements data = storeDocument.select("data");
        if (data.isEmpty()) {
            return;
        }

        Element storeElement = Iterables.get(data, 0);

        String storeName = storeElement.attr("name");
        String storeID = storeElement.attr("storeid");
        String storeAddress = storeElement.attr("address1");
        String storeCity = storeElement.attr("city");
        String storeState = storeElement.attr("state");
        String storeZip = storeElement.attr("zipcode5");

        String storeLat = null;
        String storeLong = null;
        try {
            storeLat = ((Float.parseFloat(storeElement.attr("latituderadians")) * 180) / Math.PI) + "";
            storeLong = ((Float.parseFloat(storeElement.attr("longituderadians")) * 180) / Math.PI) + "";
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        //write columns we got
        Document productDocument;
        try {
            productDocument = Jsoup.connect(API_2 + storeID).get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Elements items = productDocument.select("data[vo=item]");
        if(items.isEmpty() || items.size() < 2){
            return;
        }

        Element bbItemOne = Iterables.get(items, 0);
        Element bbItemTwo = Iterables.get(items, 1);

        //be
        beginRow();
        put(INPUT_ZIP, input_zip);
        put(STORE_ID, storeID);
        put(STORE_NAME, storeName);
        put(ADDRESS, storeAddress);
        put(CITY, storeCity);
        put(STATE, storeState);
        put(ZIP, storeZip);
        put(LAT, storeLat);
        put(LONG, storeLong);

        String image1 = bbItemOne.attr("image").replace(".jpg", ".png");
        String image2 = bbItemTwo.attr("image").replace(".jpg", ".png");

        String price1 = bbItemOne.attr("finalprice").substring(0, 4);
        String price2 = bbItemTwo.attr("finalprice").substring(0, 4);

        Double price1number = Double.parseDouble(price1);
        Double price2number = Double.parseDouble(price2);

        String scene1 = null;
        String scene2 = null;

        if( (price1number * 100) < 100){
            scene1 = "2";
        } else {
            scene1 = "1";
        }

        if( (price2number * 100) < 100){
            scene2 = "2";
        } else {
            scene2 = "1";
        }

        String[] getMoney1 = price1.split("\\.");
        String[] getMoney2 = price2.split("\\.");

        put(TITLE1, bbItemOne.attr("title"));
        put(IMAGE1, image1);
        put(PRICE1, price1);
        put(SCENESELECTOR1, scene1);
        put(DOLLAR1, getMoney1[0]);
        put(CENT1, getMoney1[1]);
        put(FINEPRINT1, bbItemOne.attr("fineprint"));
        put(DESCRIPTION1, bbItemOne.attr("description"));

        put(TITLE2, bbItemTwo.attr("title"));
        put(IMAGE2, image2);
        put(PRICE2, price2);
        put(SCENESELECTOR2, scene2);
        put(DOLLAR2, getMoney2[0]);
        put(CENT2, getMoney2[1]);
        put(FINEPRINT2, bbItemTwo.attr("fineprint"));
        put(DESCRIPTION2, bbItemTwo.attr("description"));

        put(INPUT, "be");

        saveRow();

        //brilliance
        beginRow();

        put(INPUT_ZIP, input_zip);
        put(STORE_ID, storeID);
        put(STORE_NAME, storeName);
        put(ADDRESS, storeAddress);
        put(CITY, storeCity);
        put(STATE, storeState);
        put(ZIP, storeZip);
        put(LAT, storeLat);
        put(LONG, storeLong);

        put(TITLE1, bbItemOne.attr("title"));
        put(IMAGE1, image1);
        put(PRICE1, price1);
        put(SCENESELECTOR1, scene1);
        put(DOLLAR1, getMoney1[0]);
        put(CENT1, getMoney1[1]);
        put(FINEPRINT1, bbItemOne.attr("fineprint"));
        put(DESCRIPTION1, bbItemOne.attr("description"));

        put(TITLE2, bbItemTwo.attr("title"));
        put(IMAGE2, image2);
        put(PRICE2, price2);
        put(SCENESELECTOR2, scene2);
        put(DOLLAR2, getMoney2[0]);
        put(CENT2, getMoney2[1]);
        put(FINEPRINT2, bbItemTwo.attr("fineprint"));
        put(DESCRIPTION2, bbItemTwo.attr("description"));

        put(INPUT, "brilliance");

        saveRow();

        //burgundy
        beginRow();

        put(INPUT_ZIP, input_zip);
        put(STORE_ID, storeID);
        put(STORE_NAME, storeName);
        put(ADDRESS, storeAddress);
        put(CITY, storeCity);
        put(STATE, storeState);
        put(ZIP, storeZip);
        put(LAT, storeLat);
        put(LONG, storeLong);

        put(TITLE1, bbItemOne.attr("title"));
        put(IMAGE1, image1);
        put(PRICE1, price1);
        put(SCENESELECTOR1, scene1);
        put(DOLLAR1, getMoney1[0]);
        put(CENT1, getMoney1[1]);
        put(FINEPRINT1, bbItemOne.attr("fineprint"));
        put(DESCRIPTION1, bbItemOne.attr("description"));

        put(TITLE2, bbItemTwo.attr("title"));
        put(IMAGE2, image2);
        put(PRICE2, price2);
        put(SCENESELECTOR2, scene2);
        put(DOLLAR2, getMoney2[0]);
        put(CENT2, getMoney2[1]);
        put(FINEPRINT2, bbItemTwo.attr("fineprint"));
        put(DESCRIPTION2, bbItemTwo.attr("description"));

        put(INPUT, "burgundy");

        saveRow();
    }
}
