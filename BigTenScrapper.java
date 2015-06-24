package com.eyeview.webscraper.scrapers;

import com.eyeview.data.common.DNAdFieldEnum;
import com.eyeview.locations.USZipCodesDataProvider;
import com.eyeview.soa.geozip.ZipLocation;
import com.eyeview.utils.Pair;
import com.eyeview.webscraper.scrape.enums.ElementIdentifier;
import com.eyeview.webscraper.scrape.multiscraper.BaseMultiScraper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * User: vash
 * Date: 12/8/14
 * Time: 12:16 PM
 */
public class BigTenScrapper extends BaseMultiScraper {

    // Feed Fields
    private static final String ZIP_CODE = "ev_zip_ev";
    private static final String CITY = DNAdFieldEnum.CITY.getFieldName();
    private static final String EV_LAT_EV = DNAdFieldEnum.LATITUDE.getFieldName();
    private static final String EV_LNG_EV = DNAdFieldEnum.LONGITUDE.getFieldName();
    private static final String PROVIDER_NAME_PATTERN = "ev_provider_%d_ev";
    private static final String CHANNEL_NUMBER_PATTERN = "ev_channel_number_%d_ev";


    private static final String ZAP2IT_URL = "http://tvlistings.zap2it.com/tvlistings/ZBChooseProvider.do?method=getProviders&zipcode=";
    private static final String XPATH_OF_NATGEO_CHANNEL_NUMBER = ".//a[contains(@class,zc-st-a)][text()=\"BIG10HD\"]/../../span[contains(@class,zc-st-n)]/a";

    public long getDvcProjectId() {
        return 12095l;
    }


    @Override
    public void configureScraper() {
        // Here we will add field type, input validation instructions and other stuff like that...
        disableJavaScript();
        addCollectField(ZIP_CODE, CITY, EV_LAT_EV, EV_LNG_EV);

        for (int i = 1; i < 4; i++) {
            addCollectField(String.format(PROVIDER_NAME_PATTERN, i));
            addCollectField(String.format(CHANNEL_NUMBER_PATTERN, i));
        }
    }


    @Override
    public List<Map<String, String>> getMultiplier() {
        List<Map<String, String>> paramsMapList = Lists.newArrayList();
        Map<String, ZipLocation> zipsLocations = USZipCodesDataProvider.getZipLocations();
        Map<String, Map<String, String>> allZips = USZipCodesDataProvider.getZipsByState();
        String[] stateCodeArray = {"OH", "IN", "WI", "IL", "MI", "MN"};
        List<String> stateCodeList = Arrays.asList(stateCodeArray);
        for (String state : allZips.keySet()) {
            if (stateCodeList.contains(state)) {
                log.info("State Code {} appears in our initial list of {}",state,stateCodeArray);
                for (Map.Entry<String, String> zip2city : allZips.get(state).entrySet()) {
                    String zip = zip2city.getKey();
                    String city = zip2city.getValue();
                    if (zipsLocations.containsKey(zip)) {
                        ImmutableMap<String, String> paramsMap = ImmutableMap.of(
                                ZIP_CODE, zip,
                                CITY, city,
                                EV_LAT_EV,  Double.toString(zipsLocations.get(zip).latitude),
                                EV_LNG_EV, Double.toString(zipsLocations.get(zip).longitude)
                        );
                        paramsMapList.add(paramsMap);
                    } else {
                        log.warn("zip " + zip + " is not listed in ZipLocation");
                    }

                }
            }
        }
        return paramsMapList;
    }

    protected void scrapeImpl(Map<String, String> requestData) {
        String zip = requestData.get(ZIP_CODE);
        String city = requestData.get(CITY);

        //This list of pairs, each pair contains the provider name and provider link
        List<Pair<String, String>> cableProviderLinks = extractCableProviderLinks(zip);
        Preconditions.checkState(cableProviderLinks.size() > 0, "Unable to locate at least 1 cable provider");

        beginRow();
        put(ZIP_CODE, zip);
        put(CITY, city);
        put(EV_LAT_EV, requestData.get(EV_LAT_EV));
        put(EV_LNG_EV, requestData.get(EV_LNG_EV));

        //Iterate on the cable providers and find the first three that have NatGeo channel
        int columnIndex = 1;
        Set<String> visitedProviderNames = Sets.newHashSet();
        for (Pair<String, String> cableProviderLink : cableProviderLinks) {
            String cableProviderName = cableProviderLink.first;
            if (visitedProviderNames.contains(cableProviderName)) {
                continue;
            }

            try {
                String channel = retrieveChannelNumber(cableProviderLink.second);
                put(String.format(PROVIDER_NAME_PATTERN, columnIndex), cableProviderName);
                put(String.format(CHANNEL_NUMBER_PATTERN, columnIndex), channel);
                columnIndex++;
                visitedProviderNames.add(cableProviderName);
            } catch (Exception e) {
                log.warn("Unable to locate national geo channel for zip " + zip + " provider " + cableProviderName, e);
            }

            if (columnIndex == 4) {
                log.info("Done, we found three cable providers for zip {}", zip);
                break;
            }
        }

        if (columnIndex == 1) {
            throw new RuntimeException("Unable to locate at least 1 channel for zip: " + zip);
        }

        saveRow();
    }

    private String retrieveChannelNumber(String url) {
        browser.get(url);
        WebElement element = browser.driver.findElement(By.xpath(XPATH_OF_NATGEO_CHANNEL_NUMBER));
        String channelText = element.getText();
        int channelNumber = Integer.parseInt(channelText);
        return Integer.toString(channelNumber);
    }

    //extracting the cable provider links
    private List<Pair<String, String>> extractCableProviderLinks(String zip) {
        browser.get(ZAP2IT_URL + zip);

        //retrieves the main WebElement, where all his children are provider links
        List<WebElement> mainProviderListDiv = browser.XPath.findElements("div", ElementIdentifier.CLASS, "zc-provider-list");
        WebElement mainDiv = null;
        if (!mainProviderListDiv.isEmpty()) {
            mainDiv = mainProviderListDiv.get(0);
        }

        Preconditions.checkNotNull(mainProviderListDiv, "Unable to locate provider list div for: " + zip);

        List<Pair<String, String>> providerLinks = Lists.newArrayList();
        boolean collectLinks = false;
        List<WebElement> allChildren = browser.XPath.findElements(mainDiv, "div");

        for (WebElement ChildDiv : allChildren) {
            String ChildDivText = ChildDiv.getText();
            if (ChildDivText.equalsIgnoreCase("cable")) {
                collectLinks = true;
                continue;
            }
            if (ChildDivText.equalsIgnoreCase("satellite")) {
                collectLinks = false;
                continue;
            }
            if (collectLinks) {
                WebElement childA = ChildDiv.findElement(By.tagName("a"));
                String providerLink = childA.getAttribute("href");
                String providerName = childA.getText();
                String providerNameTrimmed = providerName.indexOf(" - ") > 0 ? providerName.substring(0, providerName.indexOf(" - ")) : providerName;
                providerNameTrimmed = providerNameTrimmed.toUpperCase();
                log.info("Will add {} with link {}", providerNameTrimmed, providerLink);
                providerLinks.add(Pair.of(providerNameTrimmed, providerLink));
            }
        }
        return providerLinks;
    }

}
