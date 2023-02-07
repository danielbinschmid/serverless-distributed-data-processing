package com.function.pipelines.helper;

import com.azure.core.util.BinaryData;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class Counter {
    public static String NATION_KEY_TO_COUNT_MAP = "count";
    public static String NATION_KEY_TO_ACCOUNT_BALANCE_SUM_MAP = "sum";

    /**
     * 
     * @param file
     * @param begin - if -1, then begin at 0
     * @param end - if -1, then scan whole file
     * @return
     */
    public static Map<String, Map<String, BigDecimal>> findCountAndSum(BinaryData file, int begin, int end) {
        byte[] fileArray = file.toBytes();

        Map<String, BigDecimal> nationKeyToAccountBalanceSum = new HashMap<>();
        Map<String, BigDecimal> nationKeyToCount = new HashMap<>();
        StringBuilder temp = new StringBuilder();

        int i;
        if (begin == -1) { i = 0; } else { i = begin; }
        if (end == -1) end = fileArray.length;

        for (; i < end && i < fileArray.length; i++) {
            if (fileArray[i] == '\n') {
                if (temp.length() != 0) {
                    try {
                        countCurrent(nationKeyToAccountBalanceSum, nationKeyToCount, temp.toString());
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
                // Insert the current value to our map;
                temp = new StringBuilder();
                continue;
            }

            temp.append((char) fileArray[i]);
        }

        if (temp.length() > 1) {
            try {
                countCurrent(nationKeyToAccountBalanceSum, nationKeyToCount, temp.toString());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        Map<String, Map<String, BigDecimal>> resultMap = new HashMap<>();
        resultMap.put(NATION_KEY_TO_COUNT_MAP, nationKeyToCount);
        resultMap.put(NATION_KEY_TO_ACCOUNT_BALANCE_SUM_MAP, nationKeyToAccountBalanceSum);
        return resultMap;
    }



    private static void countCurrent(Map<String, BigDecimal> values, Map<String, BigDecimal> nationKeyToCount, String line) throws Exception {
        String[] splitArray = line.split("\\|");

        if (splitArray.length != 8) {
            throw new Exception("Wrong input");
        }

        String nationKey = splitArray[3];

        // Count how many entries from this nation there are
        BigDecimal nationCount = new BigDecimal("0");
        if (nationKeyToCount.containsKey(nationKey)) {
            nationCount = nationKeyToCount.get(nationKey);
        }
        nationCount = nationCount.add(new BigDecimal("1"));

        nationKeyToCount.put(nationKey, nationCount);

        String accountBalance = splitArray[5];

        BigDecimal curr = new BigDecimal("0.0");
        if (values.containsKey(nationKey)) {
            curr = values.get(nationKey);
        }

        curr = curr.add(new BigDecimal(accountBalance));

        values.put(nationKey, curr);
    }
}
