package com.eyeview.webscraper.scrapers;

import au.com.bytecode.opencsv.CSVReader;
import com.eyeview.locations.USZipCodesDataProvider;
import com.eyeview.soa.geozip.ZipLocation;
import com.eyeview.utils.ContainerUtil;
import com.eyeview.utils.aws.S3Manager;
import com.eyeview.utils.geo.BingGeoQuery;
import com.eyeview.utils.geo.GeoLocation;
import com.eyeview.webscraper.scrape.multiscraper.BaseMultiScraper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Uninterruptibles;
import org.openqa.selenium.*;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.interactions.Actions;
//import org.openqa.selenium.support.ui.*;


import javax.print.DocFlavor;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by vash on 4/1/15.
 */
public class HyundaiLosAngeles extends BaseMultiScraper {
    private static final String PAGE_URL = "https://www.buyhyundai.com/";

    private static final String ORIGINAL_ZIP = "ev_original_zip_ev";
    private static final String TEXT_1 = "ev_text_1_ev";

    private static final String DEALER_NAME_1 = "ev_store_name_1_ev";
    private static final String DEALER_ADDRESS_1 = "ev_store_address_1_ev";
    private static final String DEALER_CITY_1 = "ev_store_city_1_ev";
    private static final String DEALER_STATE_1 = "ev_store_state_1_ev";
    private static final String DEALER_ZIP_1 = "ev_store_zip_1_ev";

    private static final String DEALER_NAME_2 = "ev_store_name_2_ev";
    private static final String DEALER_ADDRESS_2 = "ev_store_address_2_ev";
    private static final String DEALER_CITY_2 = "ev_store_city_2_ev";
    private static final String DEALER_STATE_2 = "ev_store_state_2_ev";
    private static final String DEALER_ZIP_2 = "ev_store_zip_2_ev";

    private static final String DEALER_NAME_3 = "ev_store_name_3_ev";
    private static final String DEALER_ADDRESS_3 = "ev_store_address_3_ev";
    private static final String DEALER_CITY_3 = "ev_store_city_3_ev";
    private static final String DEALER_STATE_3 = "ev_store_state_3_ev";
    private static final String DEALER_ZIP_3 = "ev_store_zip_3_ev";

    private static final String DEALER_NAME_4 = "ev_store_name_4_ev";
    private static final String DEALER_ADDRESS_4 = "ev_store_address_4_ev";
    private static final String DEALER_CITY_4 = "ev_store_city_4_ev";
    private static final String DEALER_STATE_4 = "ev_store_state_4_ev";
    private static final String DEALER_ZIP_4 = "ev_store_zip_4_ev";

    private static final String DEALER_NAME_5 = "ev_store_name_5_ev";
    private static final String DEALER_ADDRESS_5 = "ev_store_address_5_ev";
    private static final String DEALER_CITY_5 = "ev_store_city_5_ev";
    private static final String DEALER_STATE_5 = "ev_store_state_5_ev";
    private static final String DEALER_ZIP_5 = "ev_store_zip_5_ev";

    private static final String MAP_URL = "ev_map_url_ev";
    private static final String DATA_S3_URL = "s3://eyeview-prod-assets/CLIENT_DATA/hyundai/input_nyc_phili_burg_chi_sanfran.csv";

    @Override
    public long getDvcProjectId() {
        return 17583l;
    }


    @Override
    public void configureScraper() {


        addCollectField(ORIGINAL_ZIP);
        addCollectField(TEXT_1);


        addCollectField(DEALER_NAME_1);

        addCollectField(DEALER_ADDRESS_1);
        addCollectField(DEALER_CITY_1);
        addCollectField(DEALER_STATE_1);
        addCollectField(DEALER_ZIP_1);

        addCollectField(DEALER_NAME_2);
        addCollectField(DEALER_ADDRESS_2);
        addCollectField(DEALER_CITY_2);
        addCollectField(DEALER_STATE_2);
        addCollectField(DEALER_ZIP_2);

        addCollectField(DEALER_NAME_3);
        addCollectField(DEALER_ADDRESS_3);
        addCollectField(DEALER_CITY_3);
        addCollectField(DEALER_STATE_3);
        addCollectField(DEALER_ZIP_3);

        addCollectField(DEALER_NAME_4);
        addCollectField(DEALER_ADDRESS_4);
        addCollectField(DEALER_CITY_4);
        addCollectField(DEALER_STATE_4);
        addCollectField(DEALER_ZIP_4);

        addCollectField(DEALER_NAME_5);
        addCollectField(DEALER_ADDRESS_5);
        addCollectField(DEALER_CITY_5);
        addCollectField(DEALER_STATE_5);
        addCollectField(DEALER_ZIP_5);

        addCollectField(MAP_URL);
    }

    @Override
    public List<Map<String, String>> getMultiplier() {
        List<Map<String, String>> paramsMapList = new ArrayList<>();
        try {
            File hyundaiData = S3Manager.getInstance().getFileObject(DATA_S3_URL);
            CSVReader csv = new CSVReader(new FileReader(hyundaiData));

            String[] nextline;
            while ((nextline = csv.readNext()) != null) {

                Map<String, String> zipmap = new HashMap<>();
                zipmap.put(ORIGINAL_ZIP, nextline[0]);
                zipmap.put(TEXT_1,nextline[1]);
                paramsMapList.add(zipmap);
            }
            csv.close();

        } catch (Exception e) {
            throw new RuntimeException("Error Reading File hammie", e);
        }
        return paramsMapList;
    }

    @Override
    protected void scrapeImpl(Map<String, String> requestData) {

        FirefoxDriver firefoxDriver = (FirefoxDriver) browser.driver;
        firefoxDriver.manage().timeouts().pageLoadTimeout(30, TimeUnit.SECONDS);
        firefoxDriver.manage().timeouts().implicitlyWait(25, TimeUnit.SECONDS);
        firefoxDriver.manage().timeouts().setScriptTimeout(5, TimeUnit.SECONDS);

        String originalZipcode = requestData.get(ORIGINAL_ZIP);
        String identifier = requestData.get(TEXT_1);
        browser.driver.get(PAGE_URL);


        WebElement locationChanger = firefoxDriver.findElement(By.className("changeLocation"));
        locationChanger.click();


        WebElement textbox1 = firefoxDriver.findElement(By.xpath("//*[@id=\"popupZipCode\"]/div/div/div/form/div/input"));
        textbox1.sendKeys(originalZipcode);
        WebElement submit1 = firefoxDriver.findElement(By.xpath("//*[@id=\"popupZipCode\"]/div/div/div/form/div/span/button"));
        submit1.click();

        String dealer1 = "";
        String[] addressArray1= null;

        String dealerCity1 = "";
        String dealerState1 = "";
        String dealerZip1 = "";
        String dealerAddress1 = "";

        String dealer2= "";
        String[] addressArray2= null;

        String dealerCity2 = "";
        String dealerState2 = "";
        String dealerZip2 = "";
        String dealerAddress2 = "";



        String dealer3= "";
        String[] addressArray3= null;
        String dealerCity3 = "";
        String dealerState3 = "";
        String dealerZip3 = "";
        String dealerAddress3 = "";

        String dealer4= "";
        String[] addressArray4= null;
        String dealerCity4= "";
        String dealerState4 = "";
        String dealerZip4 = "";
        String dealerAddress4 = "";

        String dealer5= "";
        String[] addressArray5= null;
        String dealerCity5 = "";
        String dealerState5 = "";
        String dealerZip5 = "";
        String dealerAddress5 = "";


        beginRow();
////*[@id="locations"]/div[1]/div/div[1]/div[2]


        List<WebElement> elements1 = firefoxDriver.findElements(By.xpath("//*[@id=\"locations\"]/div[1]"));
        dealer1 = elements1.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[1]/div[1]/div[1]/div[2]")).getText();
        String fullAddress1 = elements1.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[1]/div[1]/div[2]")).getText();
        addressArray1 = fullAddress1.split("\n");
        dealerAddress1 = addressArray1[0];

        String[] data1a = addressArray1[1].split(",");
        dealerCity1 = data1a[0];
        String[] data2a = data1a[1].split(" ");
        dealerState1 = data2a[1];
        dealerZip1 = data2a[2];


        List<WebElement> elements2 = firefoxDriver.findElements(By.xpath("//*[@id=\"locations\"]/div[2]"));
        dealer2 = elements2.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[2]/div[1]/div[1]/div[2]")).getText();
        String fullAddress2 = elements2.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[2]/div[1]/div[2]")).getText();
        addressArray2 = fullAddress2.split("\n");
        dealerAddress2 = addressArray2[0];

        String[] data1b = addressArray2[1].split(",");
        dealerCity2= data1b[0];
        String[] data2b = data1b[1].split(" ");
        dealerState2 = data2b[1];
        dealerZip2= data2b[2];

        List<WebElement> elements3 = firefoxDriver.findElements(By.xpath("//*[@id=\"locations\"]/div[3]"));
        dealer3 = elements3.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[3]/div[1]/div[1]/div[2]")).getText();
        String fullAddress3 = elements3.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[3]/div[1]/div[2]")).getText();
        addressArray3 = fullAddress3.split("\n");
        dealerAddress3 = addressArray3[0];

        String[] data1c = addressArray3[1].split(",");
        dealerCity3 = data1c[0];
        String[] data2c = data1c[1].split(" ");
        dealerState3 = data2c[1];
        dealerZip3 = data2c[2];

        List<WebElement> elements4 = firefoxDriver.findElements(By.xpath("//*[@id=\"locations\"]/div[4]"));
        dealer4 = elements4.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[4]/div[1]/div[1]/div[2]")).getText();
        String fullAddress4 = elements4.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[4]/div[1]/div[2]")).getText();
        addressArray4 = fullAddress4.split("\n");
        dealerAddress4 = addressArray4[0];

        String[] data1d = addressArray4[1].split(",");
        dealerCity4 = data1d[0];
        String[] data2d = data1d[1].split(" ");
        dealerState4 = data2d[1];
        dealerZip4 = data2d[2];

        List<WebElement> elements5 = firefoxDriver.findElements(By.xpath("//*[@id=\"locations\"]/div[5]"));
        dealer5 = elements5.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[5]/div[1]/div[1]/div[2]")).getText();
        String fullAddress5 = elements5.get(0).findElement(By.xpath("//*[@id=\"locations\"]/div[5]/div[1]/div[2]")).getText();
        addressArray5 = fullAddress5.split("\n");
        dealerAddress5 = addressArray5[0];

        String[] data1e = addressArray5[1].split(",");
        dealerCity5 = data1e[0];
        String[] data2e = data1e[1].split(" ");
        dealerState5 = data2e[1];
        dealerZip5= data2e[2];


        put(ORIGINAL_ZIP, originalZipcode);
        put(TEXT_1,identifier);



        put(DEALER_NAME_1, dealer1);
        put(DEALER_ADDRESS_1, dealerAddress1);
        put(DEALER_CITY_1, dealerCity1);
        put(DEALER_STATE_1, dealerState1);
        put(DEALER_ZIP_1, dealerZip1);

        put(DEALER_NAME_2, dealer2);
        put(DEALER_ADDRESS_2, dealerAddress2);
        put(DEALER_CITY_2, dealerCity2);
        put(DEALER_STATE_2, dealerState2);
        put(DEALER_ZIP_2, dealerZip2);

        put(DEALER_NAME_3, dealer3);
        put(DEALER_ADDRESS_3, dealerAddress3);
        put(DEALER_CITY_3, dealerCity3);
        put(DEALER_STATE_3, dealerState3);
        put(DEALER_ZIP_3, dealerZip3);

        put(DEALER_NAME_4, dealer4);
        put(DEALER_ADDRESS_4, dealerAddress4);
        put(DEALER_CITY_4, dealerCity4);
        put(DEALER_STATE_4, dealerState4);
        put(DEALER_ZIP_4, dealerZip4);


        put(DEALER_NAME_5, dealer5);

        put(DEALER_ADDRESS_5, dealerAddress5);
        put(DEALER_CITY_5, dealerCity5);
        put(DEALER_STATE_5, dealerState5);
        put(DEALER_ZIP_5, dealerZip5);


        String dealerThumb;
        WebElement zoom = firefoxDriver.findElement(By.xpath("//*[@id=\"gmap\"]/div/div[8]/div[3]/div[3]"));
        zoom.click();
        WebElement getMap = firefoxDriver.findElement(By.xpath("//*[@id=\"gmap\"]"));
        Point p = getMap.getLocation();

        File file = null;
        try {
            file = saveScreenShot(p.getX(), p.getY(), getMap.getSize().getWidth(), getMap.getSize().getHeight());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            dealerThumb = uploadAssetToMaterialsFolder(file);
        } catch (IOException e) {
            dealerThumb = "Uncaptured";
        }
        put(MAP_URL,dealerThumb);

        saveRow();





    }
}









