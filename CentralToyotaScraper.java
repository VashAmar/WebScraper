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
* User: vash
* Date: 4/10/14
* Time: 3:39 PM
*/

public class CentralToyotaScraper extends BaseMultiScraper {
    private static final String PAGE_URL = "http://www.centralatlantic.buyatoyota.com/en/dealers/index.page";

    private static final String ORIGINAL_ZIP = "ev_original_zip_ev";

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
    private static final String DATA_S3_URL = "s3://eyeview-prod-assets/CLIENT_DATA/toyota/central_atlantic_toyota_zips.csv";
    private static final String Delimiter = "s3://eyeview-prod-assets/CLIENT_DATA/toyota/delimiter.csv";
    @Override
    public long getDvcProjectId() {
        return 16806l;
    }


    @Override
    public void configureScraper() {


        addCollectField(ORIGINAL_ZIP);


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
        try{
            File toyotadata = S3Manager.getInstance().getFileObject(DATA_S3_URL);
            CSVReader csv = new CSVReader(new FileReader(toyotadata));

            String[] nextLine;
            while ((nextLine = csv.readNext()) != null){

                Map<String,String> oneMap = new HashMap<>();
                oneMap.put(ORIGINAL_ZIP,nextLine[0]);
                /*oneMap.put(DEALER_ADDRESS_1,nextLine[2]);
                oneMap.put(DEALER_CITY_1,nextLine[3]);
                oneMap.put(DEALER_STATE_1,nextLine[4]);
                oneMap.put(DEALER_ZIP_1,nextLine[5]);*/
                paramsMapList.add(oneMap);

            }
            csv.close();
        }catch(Exception e){
            throw new RuntimeException("Error reading the file.",e);
        }
        return paramsMapList;
    }

//    public List<Map<String, String>> getMultiplier() {
//            return Lists.<Map<String, String>>newArrayList(ImmutableMap.<String, String>of(
//                    ORIGINAL_ZIP, "07607"
//            ));
//    }

    @Override
    protected void scrapeImpl(Map<String, String> requestData) {

        FirefoxDriver firefoxDriver = (FirefoxDriver) browser.driver;
        firefoxDriver.manage().timeouts().pageLoadTimeout(30, TimeUnit.SECONDS);
        firefoxDriver.manage().timeouts().implicitlyWait(25, TimeUnit.SECONDS);
        firefoxDriver.manage().timeouts().setScriptTimeout(5, TimeUnit.SECONDS);

        String originalZipcode = requestData.get(ORIGINAL_ZIP);
        browser.driver.get(PAGE_URL);

        WebElement popup = firefoxDriver.findElement(By.xpath("//*[@id=\"zip-modal-prompt\"]/div/div/div[3]/div[1]/button"));
        popup.click();
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        WebElement a = firefoxDriver.findElement(By.id("search-value"));
        a.clear();
        a.sendKeys(originalZipcode);
        WebElement searchButton = firefoxDriver.findElement(By.id("search-submit"));
        searchButton.click();

        //a.submit();
        WebElement button = firefoxDriver.findElement(By.id("search-more-dealers"));
        button.click();


        List<WebElement> elements = firefoxDriver.findElements(By.className("result"));

        String dealer = "";
        String[] address =null;
        String dealerCity = "";
        String dealerState = "";
        String dealerZip = "";
        String dealerAddress= "";
        beginRow();
        put(ORIGINAL_ZIP, originalZipcode);

        for(int i = 0; i<5; i++){


            if(i<elements.size()){
                try {

                dealer = elements.get(i).findElement(By.cssSelector("h2[class='h3 marg-bottom-2-sm']")).getText();
                String fullAddress = elements.get(i).findElement(By.className("address")).getText();
                    address = fullAddress.split(",");
                    String data = address[0];
                    String data2 = address[1];
                    String[] segments = data.split(" ");
                    int arrayLen= segments.length;
                    dealerZip = data2.split(" ")[2];
                    dealerState =data2.split(" ")[1];
                    String first ="";
                    Set<String> delimSet = new HashSet<>(150);

                    try{
                        File delimData = S3Manager.getInstance().getFileObject(Delimiter);
                        CSVReader csv = new CSVReader(new FileReader(delimData));

                        String[] nextLine;
                        while ((nextLine = csv.readNext()) != null){

                            delimSet.add(nextLine[0]);
                        }
                        csv.close();
                        if(delimSet.contains(segments[arrayLen-1])){
                            dealerCity = segments[arrayLen-1];
                            for (int j=0; j<segments.length-1;j++){
                                first =first + segments[j]+ " ";
                            }

                        }

                        else if(delimSet.contains(segments[arrayLen-2]+" "+segments[arrayLen-1])){
                            dealerCity = segments[arrayLen-2]+" "+segments[arrayLen-1];
                            for (int k=0; k<segments.length-2;k++){
                                first =first + segments[k]+ " ";


                            }
                        }
                        dealerAddress=  first;
                    }catch(Exception e){
                        throw new RuntimeException("fml",e);
                    }

                if(i==0){

                    put(DEALER_NAME_1,dealer);
                    put(DEALER_ADDRESS_1,dealerAddress);
                    put(DEALER_CITY_1,dealerCity);
                    put(DEALER_STATE_1,dealerState);
                    put(DEALER_ZIP_1,dealerZip);
                }
                if(i==1){
                    put(DEALER_NAME_2,dealer);
                    put(DEALER_ADDRESS_2,dealerAddress);
                    put(DEALER_CITY_2,dealerCity);
                    put(DEALER_STATE_2,dealerState);
                    put(DEALER_ZIP_2,dealerZip);
                }
                if(i==2){
                    put(DEALER_NAME_3,dealer);
                    put(DEALER_ADDRESS_3,dealerAddress);
                    put(DEALER_CITY_3,dealerCity);
                    put(DEALER_STATE_3,dealerState);
                    put(DEALER_ZIP_3,dealerZip);
                }
                if(i==3){
                    put(DEALER_NAME_4,dealer);
                    put(DEALER_ADDRESS_4,dealerAddress);
                    put(DEALER_CITY_4,dealerCity);
                    put(DEALER_STATE_4,dealerState);
                    put(DEALER_ZIP_4,dealerZip);
                }
                if(i==4){
                    put(DEALER_NAME_5,dealer);
                    put(DEALER_ADDRESS_5,dealerAddress);
                    put(DEALER_CITY_5,dealerCity);
                    put(DEALER_STATE_5,dealerState);
                    put(DEALER_ZIP_5,dealerZip);

                }
            } catch (NullPointerException e){
                    System.out.println(" error" + e);
                }

        }
        String dealerThumb;

        WebElement map = firefoxDriver.findElement(By.xpath("//*[@id=\"result-map-wrap\"]/div[2]"));
        map.click();
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        WebElement getmap = firefoxDriver.findElement(By.xpath("//*[@id=\"result-map-wrap\"]"));
        Point p = getmap.getLocation();

        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);

        File file = null;
        try {
            file = saveScreenShot(p.getX(), p.getY(), getmap.getSize().getWidth(), getmap.getSize().getHeight());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            dealerThumb = uploadAssetToMaterialsFolder(file);
        } catch (IOException e) {
            dealerThumb = "Uncaptured";
        }

        WebElement mapClose = firefoxDriver.findElement(By.cssSelector("div[class='role-link map-expand']"));
        mapClose.click();


        put(MAP_URL, dealerThumb);

    }
        saveRow();
}
}
