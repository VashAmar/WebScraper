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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * User: vash
 * Date: 3/21/14
 * Time: 11:02 AM
 */

public class BRU extends BaseMultiScraper {

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
    private static String BUYNOW_1 = "ev_buynow_1_ev";

    //APIs
    private static String API_1 = "http://api.shoplocal.com/babiesrus/2012.2/xml/getstores.aspx?campaignid=16ec87119a9a29b6&citystatezip=";

    private static String API_2 = "http://api.shoplocal.com/babiesrus/2012.2/xml/getretailertaglistings.aspx?campaignid=16ec87119a9a29b6&resultset=full&listingimagewidth=300&retailertagid=2862&pd=46B1B04C3D17EFABD4AC853C4BC6E232B2D5D393886015B49E685759478E60D84C57816A8FED0DF553CFE1D8001910F7&storeid=";

    @Override
    public long getDvcProjectId() {
        return 16010l;
    }

    @Override
    public void configureScraper() {
        disableJavaScript();
        addCollectField(ZIP, STORE_NAME, STORE_ID, ADDRESS_1, ADDRESS_2, CITY, STATE, POSTALCODE, LATITUDE, LONGITUDE, TITLE_1, IMAGE_1, PRICE_1, BUYNOW_1);
    }

//       public List<Map<String, String>> getMultiplier() {
//        return Lists.<Map<String, String>>newArrayList(ImmutableMap.<String, String>of(
//                ZIP, "07607"
//        ));
//        }

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
        String zip = requestData.get(ZIP);
        put(ZIP, zip);
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

        Document getItems;
        try {
            getItems = Jsoup.connect(API_2 + store.attr("storeid")).get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Element check;
        Elements checker;

        try{
            checker = getItems.select("collection");
            check = checker.first();

            if("0".equals(check.attr("count"))){
                return;
            }

        } catch (Exception e){
            throw new RuntimeException(e);
        }

        Elements items = getItems.select("data");
        Element item = items.first();

        String image, imageReplaced;
        image = items.attr("image");
        imageReplaced = image.replace("jpg", "png");

        put(TITLE_1, item.attr("title"));
        put(IMAGE_1, imageReplaced);
        put(PRICE_1, item.attr("price"));
        put(BUYNOW_1, "http://weeklyad.babiesrus.com/babiesrus/listingdetail?listingid=" + item.attr("listingid"));

        saveRow();
    }
}