package com.eyeview.webscraper.scrapers;

import au.com.bytecode.opencsv.CSVReader;
import com.eyeview.locations.USZipCodesDataProvider;
import com.eyeview.soa.geozip.ZipLocation;
import com.eyeview.utils.aws.S3Manager;
import com.eyeview.webscraper.scrape.multiscraper.BaseMultiScraper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * User: vash
 * Date: 3/21/14
 * Time: 11:02 AM
 */

public class BRU3 extends BaseMultiScraper {

    //member variables
    private static String ZIP = "ev_zip_ev";
    private static String STORE_NAME = "ev_store_name_ev";
    private static String STORE_ID = "ev_store_id_ev";
    private static String ADDRESS_1 = "ev_address_1_ev";
    private static String ADDRESS_2 = "ev_address_2_ev";
    private static String CITY = "ev_city_ev";
    private static String STATE = "ev_state_ev";
    private static String POSTALCODE = "ev_postalcode_ev";
    private static String LATITUDE = "ev_latitude_ev";
    private static String LONGITUDE = "ev_longitude_ev";
    private static String TITLE_1 = "ev_title_1_ev";
    private static String IMAGE_1 = "ev_image_1_ev";
    private static String PRICE_1 = "ev_price_1_ev";
    private static String CLICK_1 = "ev_CLICK_1_ev";
    private static String TITLE_2 = "ev_title_2_ev";
    private static String IMAGE_2 = "ev_image_2_ev";
    private static String PRICE_2 = "ev_price_2_ev";
    private static String CLICK_2 = "ev_CLICK_2_ev";
    private static String TITLE_3 = "ev_title_3_ev";
    private static String IMAGE_3 = "ev_image_3_ev";
    private static String PRICE_3 = "ev_price_3_ev";
    private static String CLICK_3 = "ev_CLICK_3_ev";

    //APIs
    private static String API_1 = "http://api.shoplocal.com/babiesrus/2012.2/xml/getstores.aspx?campaignid=16ec87119a9a29b6&citystatezip=";

    private static String API_2 = "http://api.shoplocal.com/babiesrus/2012.2/xml/getretailertaglistings.aspx?campaignid=16ec87119a9a29b6" +
            "&resultset=full&listingimagewidth=300&retailertagid=2862&storeid=";

    private static String API_3 = "http://api.shoplocal.com/babiesrus/2012.2/xml/getretailertaglistings.aspx?campaignid=16ec87119a9a29b6" +
            "&resultset=full&listingimagewidth=300&retailertagid=2864&storeid=";

    private static String HASH = "&pd=46B1B04C3D17EFABD7A58C3B4EC0E330B2D5D393886015B49E685759408E64C45155806F9BF419E853D2FCD810781C";

    private static String CLICK = "http://weeklyad.babiesrus.com/babiesrus/?listingid=";

    private static String CLICK2 = "?camp=OLADV:Eyeview:BRU:VideoCircular:PreRoll:040614";

    @Override
    public long getDvcProjectId() {
        return 16114l;
    }

    @Override
    public void configureScraper() {
        disableJavaScript();
        addCollectField(ZIP, STORE_NAME, STORE_ID, ADDRESS_1, ADDRESS_2, CITY, STATE, POSTALCODE, LATITUDE, LONGITUDE, TITLE_1, IMAGE_1, PRICE_1, CLICK_1, TITLE_2, IMAGE_2, PRICE_2, CLICK_2, TITLE_3, IMAGE_3, PRICE_3, CLICK_3);
    }

//        public List<Map<String, String>> getMultiplier() {
//        return Lists.<Map<String, String>>newArrayList(ImmutableMap.<String, String>of(
//                ZIP, "10003"
//        ));
//    }

    @Override
    public List<Map<String, String>> getMultiplier() {
        List<ZipLocation> zipLocationsList = USZipCodesDataProvider.getZipLocationsList();
        List<Map<String, String>> paramsMapList = Lists.newArrayListWithCapacity(zipLocationsList.size());
        for (ZipLocation zipLocation : zipLocationsList) {
            ImmutableMap<String, String> paramsMap = ImmutableMap.of(
                    ZIP, zipLocation.zip
            );
            paramsMapList.add(paramsMap);
        }
        return paramsMapList;
    }

    protected void scrapeImpl(Map<String, String> requestData){
        //write columns we have
        String zip = requestData.get(ZIP);
        put(ZIP, zip);

        //add Selenium or Jsoup

        Document getStoresDocument;
        try {
            getStoresDocument = Jsoup.connect(API_1 + zip).get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Elements data = getStoresDocument.select("data");

        Element store;
        Iterator<Element> dataListIterator = data.iterator();
        store = dataListIterator.next();

        //add API calls

        //write columns we have

        put(STORE_NAME, store.attr("name"));
        put(STORE_ID, store.attr("storeid"));
        put(ADDRESS_1, store.attr("address1"));
        put(ADDRESS_2, store.attr("address2"));
        put(CITY, store.attr("city"));
        put(STATE, store.attr("state"));
        put(POSTALCODE, store.attr("postalcode"));

        String storeLatitude = store.attr("latituderadians").trim();
        String storeLongitude = store.attr("longituderadians").trim();

        storeLatitude = String.valueOf((Float.parseFloat(storeLatitude) * 180) / Math.PI);
        storeLongitude = String.valueOf((Float.parseFloat(storeLongitude) * 180) / Math.PI);


        put(LATITUDE, storeLatitude);
        put(LONGITUDE, storeLongitude);

//        Document getItems;
//        try {
//            getItems = Jsoup.connect(API_2 + store.attr("storeid") + HASH).get();
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//        Elements items = getItems.select("data");
//
//        List<Element> itemList = Lists.newArrayList(items.iterator());

        Document getNormalItems;
        try {
            getNormalItems = Jsoup.connect(API_3 + store.attr("storeid") + HASH).get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Element item1, item2, item3;

        Elements normalItems = getNormalItems.select("data");
        List<Element> normalList = Lists.newArrayList(normalItems.iterator());

        item1 = normalList.get(0);

        String image, title, price, listingID;

        image = item1.attr("image");
        title = item1.attr("title");
        price = item1.attr("price");
        listingID = item1.attr("listingid");

        put(TITLE_1, title);
        put(IMAGE_1, image);
        put(PRICE_1, price);
        put(CLICK_1, CLICK + listingID + CLICK2);

        item2 = normalList.get(1);
        item3 = normalList.get(2);

        image = item2.attr("image");
        title = item2.attr("title");
        price = item2.attr("price");
        listingID = item2.attr("listingid");

        put(TITLE_2, title);
        put(IMAGE_2, image);
        put(PRICE_2, price);
        put(CLICK_2, CLICK + listingID + CLICK2);

        image = item3.attr("image");
        title =  item3.attr("title");
        price = item3.attr("price");
        listingID = item3.attr("listingid");

        put(TITLE_3, title);
        put(IMAGE_3, image);
        put(PRICE_3, price);
        put(CLICK_3, CLICK + listingID + CLICK2);

        saveRow();
    }
}