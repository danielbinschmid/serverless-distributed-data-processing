package com.function.helper;

import com.function.config.AccountConfig;
import org.json.JSONObject;

public class Partitioner {

    public static JSONObject getIthPartition(int index, byte[] content, String filename) {
        // Find range length for n partitions
        int rangeLength = content.length / AccountConfig.N_PARTITIONS;

        // If it is the first partition, only find the range end
        if (index == 0) {
            // The range end is in this case index + rangeLength
            int rangeEnd = getIndexOfNextLineFrom(index + rangeLength, content);
            return buildJSON(filename, index, rangeEnd);
        }
        // If it is the last partition
        if (index == AccountConfig.N_PARTITIONS - 1) {
            // The last partition starts and i * range length
            int rangeStart = getIndexOfNextLineFrom(index*rangeLength, content);
            // and ends at the end of the file
            int rangeEnd = content.length;
            return buildJSON(filename, rangeStart, rangeEnd);
        }

        // For the ith partition -> it starts at i * range length
        int rangeStart = getIndexOfNextLineFrom(index*rangeLength, content);
        // and ends at (i + 1) -> range length
        int rangeEnd = getIndexOfNextLineFrom((index + 1)*rangeLength, content);

        return buildJSON(filename, rangeStart, rangeEnd);
    }

    private static int getIndexOfNextLineFrom(int index, byte[] content) {
        for (; index < content.length; index++) {
            if (content[index] == '\n') {
                break;
            }
        }
        // Returns the first byte of the next line
        return index + 1;
    }

    private static JSONObject buildJSON(String filename, int rangeStart, int rangeEnd) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(AccountConfig.AGGREGATION_JOB_TARGET, filename);
        jsonObject.put(AccountConfig.AGGREGATION_JOB_RANGE_START, rangeStart);
        jsonObject.put(AccountConfig.AGGREGATION_JOB_RANGE_END, rangeEnd);

        return jsonObject;
    }
}
