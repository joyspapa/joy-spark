package com.obzen.spark.batch.inykang.model;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by OBZEN on 2017-02-13.
 */
public class ChannelCode implements Serializable {
    private static final Logger logger = Logger.getLogger(ChannelCode.class);
    private static final String DELIMITER = ";";
    private static final String CHN_CODE_FILE = "/app/ecube/opt/spark/data/VST_CHN_COD";

    private Map<String, List<String>> dsmediaMap = new HashMap<>();
    private Map<String, List<String>> channelIDMap = new HashMap<>();

    //    static {
//        initialLoad();
//    }
    public ChannelCode(List<String[]> chnCodList) {
        initialLoad(chnCodList);
    }

    public Map<String, List<String>> getDsmediaMap() {
        return dsmediaMap;
    }

    public Map<String, List<String>> getChannelIDMap() {
        return channelIDMap;
    }

    public String getChannelGubunWithDsmedia(String dsmedia) {
        List<String> channelList = dsmediaMap.get(dsmedia);
        if (channelList == null || channelList.isEmpty()) {
            logger.warn("cannot find channel gubun! dsmedia: " + dsmedia);
            return "";
        } else
            return channelList.get(0);
    }

    public String getChannelIDWithDsmedia(String dsmedia) {
        List<String> channelList = dsmediaMap.get(dsmedia);
        if (channelList == null || channelList.isEmpty()) {
            logger.warn("cannot find channel ID! dsmedia: " + dsmedia);
            return "";
        } else
            return channelList.get(1);
    }

    public String getChannelGubun(String channelID) {
        List<String> channelList = channelIDMap.get(channelID);
        if (channelID == null || channelID.trim().equals("") ||
                channelList == null || channelList.isEmpty())
            return "-";
        else
            return channelList.get(0);
    }

    public void initialLoad(List<String[]> chnCodList) {
        logger.info(String.format("VST_CHN_COD is loading from Hadoop..."));

        for (String[] data : chnCodList) {
            dsmediaMap.put(data[3], Arrays.asList(new String[]{data[0] + "-" + data[2], data[1]}));
            channelIDMap.put(data[1], Arrays.asList(new String[]{data[0] + "-" + data[2]}));
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format(
                    "dsmediaMap: %s\nchannelIDMap: %s"
                    , dsmediaMap.toString()
                    , channelIDMap.toString()
            ));
        }
    }

    private void initialLoad() {
        File file = new File(CHN_CODE_FILE);
        logger.info(String.format("%s is loading...", file.getName()));
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));

            String line;
            int i = 0;
            while ((line = reader.readLine()) != null) {
                if (i > 0) {
                    String[] data = line.split(DELIMITER, -1);
                    dsmediaMap.put(data[3], Arrays.asList(new String[]{data[0] + "-" + data[2], data[1]}));
                    channelIDMap.put(data[1], Arrays.asList(new String[]{data[0] + "-" + data[2]}));
                }
                i++;
            }
        } catch (Exception _ex) {
            logger.error("Error in initial loading...", _ex);
        } finally {
            if (reader != null) ;
        }
        try {
            reader.close();
        } catch (IOException e) {
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format(
                    "dsmediaMap: %s\nchannelIDMap: %s"
                    , dsmediaMap.toString()
                    , channelIDMap.toString()
            ));
        }
    }
}
