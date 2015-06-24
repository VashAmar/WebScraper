package com.eyeview.webscraper.scrapers;

import au.com.bytecode.opencsv.CSVReader;
import com.eyeview.utils.aws.S3Manager;
import com.eyeview.utils.geo.BingGeoQuery;
import com.eyeview.utils.geo.GeoLocation;
import com.eyeview.webscraper.scrape.multiscraper.BaseMultiScraper;
import com.google.common.collect.Maps;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Created with IntelliJ IDEA.
 * User: vash
 * Date: 4/18/14
 * Time: 5:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class BeatsByScrape extends BaseMultiScraper {
    //INPUT FEED - CHANGE THIS FOR EVERY SCRAPER
    private static String INPUT_FEED = "s3://eyeview-prod-assets/CLIENT_DATA/beats/input.csv";

    //member variables
    private static String ZIPCODE = "ev_zipCode_ev";
    private static String STORENAME = "ev_storeName_ev";
    private static String ADDRESS = "ev_address_ev";
    private static String CITY  = "ev_city _ev";
    private static String ZIP = "ev_zip_ev";
    private static String LATITUDE = "ev_latitude_ev";
    private static String LONGITUDE = "ev_longitude_ev";

    //APIs
    private static String API_1 = "http://www.currys.co.uk/gbuk/s/find-a-store.html";

    @Override
    public long getDvcProjectId() {
        return 16222l;
    }


    @Override
    public void configureScraper() {
        // disableJavaScript();
        addCollectField(ZIPCODE, STORENAME, ADDRESS, CITY, ZIP, LATITUDE, LONGITUDE);
    }

    /* List<Map<String, String>> getMultiplier() {
    return Lists.<Map<String, String>>newArrayList(ImmutableMap.<String, String>of(
    TEST KEY, TEST VALUE
    ));
    }*/

    @Override
    public List<Map<String, String>> getMultiplier() {
        List<Map<String,String>> theList = new LinkedList<>();

        try{
            File csvFile = S3Manager.getInstance().getFileObject(INPUT_FEED);
            CSVReader csvReader = new CSVReader(new FileReader(csvFile));
            String [] nextLine;
            while( (nextLine = csvReader.readNext()) != null ){
                Map<String, String> aRow = Maps.newHashMap();

                aRow.put(CITY, nextLine[0]);
                theList.add(aRow);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to load baseline CSV");
        }
        return theList;
    }

    protected void scrapeImpl(Map<String, String> requestData){

        //write columns we have
        String cityIn = requestData.get(CITY);

        //add Selenium or Jsoup
        FirefoxDriver firefoxDriver = (FirefoxDriver) browser.driver;

        firefoxDriver.manage().timeouts().pageLoadTimeout(30, TimeUnit.SECONDS);
        firefoxDriver.manage().timeouts().implicitlyWait(25, TimeUnit.SECONDS);
        firefoxDriver.manage().timeouts().setScriptTimeout(5, TimeUnit.SECONDS);

        browser.driver.get(API_1);

        WebElement input = firefoxDriver.findElement(By.xpath("(//*[@id='sStoreKeyword'])[1]"));
        WebElement button = firefoxDriver.findElement(By.xpath("(//*[@class='btn btnBold'])[1]"));
        input.clear();
        input.clear();
        input.sendKeys(cityIn);
        button.click();

        List<WebElement> elements = browser.driver.findElements(By.className("address"));

        for(WebElement element: elements){

            System.out.print(elements.toString());

            String storeName, address, city, state, zip, lat, longitude;
            String inputString = element.getText();


            String[] resultsSplit = inputString.split("\n");
            String[] addressSplit = resultsSplit[2].split(",");

            storeName = resultsSplit[0];
            address = resultsSplit[1];
            city = addressSplit[0];
            zip = addressSplit[1];

            //write columns we have
            put(STORENAME, storeName);
            put(ADDRESS, address);
            put(CITY, city);

            GeoLocation loc = BingGeoQuery.geoCodeAddress(address + "," + addressSplit[0] + "." + addressSplit[1]);
            lat = String.valueOf(loc.latitude);
            longitude = String.valueOf(loc.longitude);

            put(ZIP, zip.trim());
            put(LATITUDE, lat);
            put(LONGITUDE, longitude);
            saveRow();
        }
    }
}