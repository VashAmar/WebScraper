package com.eyeview.webscraper.scrapers;

import au.com.bytecode.opencsv.CSVReader;
import com.eyeview.data.common.DNAdFieldEnum;
import com.eyeview.utils.aws.S3Manager;
import com.eyeview.webscraper.scrape.multiscraper.BaseMultiScraper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.Map;

/**
 * User: vash
 * Date: 5/21/14
 * Time: 3:12 PM
 */
public class CarrabasScraper extends BaseMultiScraper {

   private static final String CSV_S3_URL = "s3://eyeview-prod-assets/CLIENT_DATA/carrabas/carrabbas_prepared_feed_oct27.csv";
   //private static final String CSV_S3_URL = "/home/raya/Desktop/Carrabbas/feed_S3.csv";

    //Original Feed fields
//    private static final String STORE_NUMBER = "ev_store_number_ev";
//    private static final String STORE_NAME = "ev_store_name_ev";
    private static final String ADDRESS = DNAdFieldEnum.ADDRESS.getFieldName();
    private static final String CITY = DNAdFieldEnum.CITY.getFieldName();
    private static final String STATE_CODE = DNAdFieldEnum.STATECODE.getFieldName();
    private static final String ZIP_CODE = DNAdFieldEnum.ZIPCODE.getFieldName();
    private static final String LATITUDE = DNAdFieldEnum.LATITUDE.getFieldName();
    private static final String LONGITUDE = DNAdFieldEnum.LONGITUDE.getFieldName();

    //    private static final String WEATHER_MSG = "ev_weather_msg_ev";
//    private static final String EXTRA_MSG_1 = "ev_extra_msg_1_ev";
//    private static final String EXTRA_MSG_2 = "ev_extra_msg_2_ev";
    private static final String CLICK_URL = "ev_click_url_ev";
    private static final String ACTIVE_FROM = DNAdFieldEnum.ACTIVE_FROM.getFieldName();
    private static final String ACTIVE_TO = DNAdFieldEnum.ACTIVE_TO.getFieldName();
    private static final String DAY = "ev_day_ev";
    private static final String TIME = "ev_time_ev";
    private static final String TIME_MSG = "ev_timemessage_ev";
    private static final String INPUT = "ev_input_ev";
    private static final String DIRECT_TARGETING_KEY = "ev_direct_targeting_key_ev";

    private static final String DAY_DIFF = "ev_daydiff_ev";


    @Override
    public long getDvcProjectId() {
        //return 13966l;
        return 14570l;
    }

    @Override
    public void configureScraper() {
        disableJavaScript();
        disableFeedSortFlag();
        addCollectField(
                //Original Feed fields
                ADDRESS, CITY, STATE_CODE, ZIP_CODE, LATITUDE, LONGITUDE, CLICK_URL, ACTIVE_FROM, ACTIVE_TO, DAY, TIME, INPUT, TIME_MSG, DIRECT_TARGETING_KEY
        );
    }

    @Override
    public List<Map<String, String>> getMultiplier() {

       List<Map<String, String>> storesOneWeek = getMapsForOneWeek();

       List<Map<String, String>> storesFortyWeeks = storesOneWeek;

       for(int i=1; i<=35; i++){


           List<Map<String, String>> storesOneWeekTemp = Lists.newArrayList();

           for(Map<String, String> stores : storesOneWeek){
               Map<String, String> tempStoresOneWeek = Maps.newHashMap();

               tempStoresOneWeek.put("ev_zipcode_ev",stores.get("ev_zipcode_ev"));
               tempStoresOneWeek.put("ev_address_ev",stores.get("ev_address_ev"));
               tempStoresOneWeek.put("ev_day_ev",stores.get("ev_day_ev"));
               tempStoresOneWeek.put("ev_city_ev",stores.get("ev_city_ev"));
               tempStoresOneWeek.put("ev_longitude_ev",stores.get("ev_longitude_ev"));
               tempStoresOneWeek.put("ev_timemessage_ev",stores.get("ev_timemessage_ev"));
               tempStoresOneWeek.put("ev_statecode_ev",stores.get("ev_statecode_ev"));
               tempStoresOneWeek.put("ev_input_ev",stores.get("ev_input_ev"));
               tempStoresOneWeek.put("ev_click_url_ev",stores.get("ev_click_url_ev"));
               tempStoresOneWeek.put("ev_time_ev",stores.get("ev_time_ev"));
               tempStoresOneWeek.put("ev_activeto_ev",stores.get("ev_activeto_ev"));
               tempStoresOneWeek.put("ev_activefrom_ev",stores.get("ev_activefrom_ev"));
               tempStoresOneWeek.put("ev_latitude_ev",stores.get("ev_latitude_ev"));
               tempStoresOneWeek.put("ev_direct_targeting_key_ev",stores.get("ev_direct_targeting_key_ev"));

               String dayDiffPrevValue = stores.get("ev_daydiff_ev");
               tempStoresOneWeek.put("ev_daydiff_ev",String.valueOf(Integer.valueOf(dayDiffPrevValue)+7));

               storesOneWeekTemp.add(tempStoresOneWeek);
           }

       storesFortyWeeks.addAll(storesOneWeekTemp);

       storesOneWeek = storesOneWeekTemp;
       }

        return storesFortyWeeks;
    }

    private List<Map<String, String>> getMapsForOneWeek() {
        List<Map<String, String>> stores = Lists.newLinkedList();

        int dayDiff = -1;

        try {
            File theatreList = S3Manager.getInstance().getFileObject(CSV_S3_URL);
            CSVReader csv = new CSVReader(new FileReader(theatreList));

            //CSVReader csv = new CSVReader(new FileReader("/home/raya/Desktop/stories/Carrabbas/Carrabas05.08.13/carrabbas_prepared_feed.csv"));

            String[] nextLine;
            while ((nextLine = csv.readNext()) != null) {

                Map<String, String> storeMap = Maps.newHashMap();

                storeMap.put(ADDRESS, nextLine[0]);
                storeMap.put(CITY, nextLine[1]);
                storeMap.put(STATE_CODE, nextLine[2]);
                storeMap.put(ZIP_CODE, nextLine[3]);
                storeMap.put(LATITUDE, nextLine[4]);
                storeMap.put(LONGITUDE, nextLine[5]);
                storeMap.put(CLICK_URL, nextLine[6]);
                storeMap.put(ACTIVE_FROM, nextLine[7]);
                storeMap.put(ACTIVE_TO, nextLine[8]);
                storeMap.put(DAY, nextLine[9]);
                storeMap.put(TIME, nextLine[10]);
                storeMap.put(INPUT, nextLine[11]);
                storeMap.put(TIME_MSG, nextLine[12]);
                storeMap.put(DIRECT_TARGETING_KEY, nextLine[13]);

                if ("mon".equals(nextLine[12].trim())) {
                    DateTime activeFromTime = new DateTime(nextLine[7], DateTimeZone.UTC);
                    DateTime now = new DateTime(DateTimeZone.UTC);
                    Days days = Days.daysBetween(activeFromTime, now);
                    dayDiff = days.getDays() - 6;
                }

                stores.add(storeMap);
            }

            csv.close();
            theatreList.delete();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get Carrabas restaurant", e);
        }

        //Preconditions.checkState(dayDiff > 0);

        for (Map<String, String> storeMap : stores) {
            storeMap.put(DAY_DIFF, String.valueOf(dayDiff));
        }

        return stores;
    }

    @Override
    protected void scrapeImpl(Map<String, String> requestData) {
        String address = requestData.get(ADDRESS);
        String city = requestData.get(CITY);
        String stateCode = requestData.get(STATE_CODE);
        String zipCode = requestData.get(ZIP_CODE);
        String latitude = requestData.get(LATITUDE);
        String longitude = requestData.get(LONGITUDE);
        String clickUrl = requestData.get(CLICK_URL);
        String activeFrom = requestData.get(ACTIVE_FROM);
        String activeTo = requestData.get(ACTIVE_TO);
        String day = requestData.get(DAY);
        String time = requestData.get(TIME);
        String input = requestData.get(INPUT);
        String timeMsg = requestData.get(TIME_MSG);
        String directTargetingKey = requestData.get(DIRECT_TARGETING_KEY);

        int dayDiff = Integer.valueOf(requestData.get(DAY_DIFF));

        DateTime activeFromTime = new DateTime(activeFrom, DateTimeZone.UTC); //2013, 2, 17, 0, 0, 0, 0, DateTimeZone.UTC);
        DateTime activeToTime = new DateTime(activeTo, DateTimeZone.UTC);

        activeFromTime = activeFromTime.plusDays(dayDiff);
        activeToTime = activeToTime.plusDays(dayDiff);

        beginRow();

        put(ADDRESS, address);
        put(CITY, city);
        put(STATE_CODE, stateCode);
        put(ZIP_CODE, zipCode);
        put(LATITUDE, latitude);
        put(LONGITUDE, longitude);
        put(CLICK_URL, clickUrl);
        put(ACTIVE_FROM, activeFromTime.toDateTimeISO().toString().replaceFirst(".000", ""));
        put(ACTIVE_TO, activeToTime.toDateTimeISO().toString().replaceFirst(".000", ""));
        put(DAY, day);
        put(TIME, time);
        put(INPUT, input);
        put(TIME_MSG, timeMsg);
        put(DIRECT_TARGETING_KEY, directTargetingKey);

        saveRow();
    }
}