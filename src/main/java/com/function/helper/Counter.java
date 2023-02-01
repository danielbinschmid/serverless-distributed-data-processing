package com.function.helper;

import com.azure.core.util.BinaryData;

import org.javatuples.Pair;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class Counter {
    public static Integer NATION_KEY_TO_COUNT_MAP = 0;
    public static Integer NATION_KEY_TO_ACCOUNT_BALANCE_SUM_MAP = 1;

    public static Map<String, Pair<Double, BigDecimal>> findCountAndSumQueue(BinaryData file, long begin, long end) {
        byte[] fileArray = file.toBytes();
        long counter = 0;

        // Skip the lines until we reach the begin line
        int i = 0;
        for (; i < fileArray.length; i++) {
            if (counter == begin) break;
            if (fileArray[i] == '\n') counter++;
        }

        Map<String, Pair<Double, BigDecimal>> nationToSumCount = new HashMap<>();
        // Continue to the ones that are in our interest
        StringBuilder temp = new StringBuilder();
        for (; i < fileArray.length; i++) {
            if (counter > end) {
                break;
            }

            if (fileArray[i] == '\n') {
                // Insert the current value to our map;
                if (temp.length() != 0) {
                    try {
                        countCurrentQueue(nationToSumCount, temp.toString());
                    } catch (Exception e) {
                        System.err.println(e.getMessage());
                    }
                }
                counter++;
                temp = new StringBuilder();
                continue;
            }
            temp.append((char) fileArray[i]);
        }   

        if (temp.length() != 0) {
            try {
                countCurrentQueue(nationToSumCount, temp.toString());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        return nationToSumCount;
    }

    public static Map<Integer, Map<String, BigDecimal>> findCountAndSumBlob(BinaryData file, int begin, int end) {
        byte[] fileArray = file.toBytes();

        Map<String, BigDecimal> nationKeyToAccountBalanceSum = new HashMap<>();
        Map<String, BigDecimal> nationKeyToCount = new HashMap<>();
        StringBuilder temp = new StringBuilder();

        int i = begin;
        for (; i < end && i < fileArray.length; i++) {
            if (fileArray[i] == '\n') {
                if (temp.length() != 0) {
                    try {
                        countCurrentBlob(nationKeyToAccountBalanceSum, nationKeyToCount, temp.toString());
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
                countCurrentBlob(nationKeyToAccountBalanceSum, nationKeyToCount, temp.toString());
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        Map<Integer, Map<String, BigDecimal>> resultMap = new HashMap<>();
        resultMap.put(NATION_KEY_TO_COUNT_MAP, nationKeyToCount);
        resultMap.put(NATION_KEY_TO_ACCOUNT_BALANCE_SUM_MAP, nationKeyToAccountBalanceSum);
        return resultMap;
    }

    private static void countCurrentBlob(Map<String, BigDecimal> values, Map<String, BigDecimal> nationKeyToCount, String line) throws Exception {
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

    private static void countCurrentQueue(Map<String, Pair<Double, BigDecimal>> nationToSumCount, String line) throws Exception {
        String[] splitArray = line.split("\\|");

        if (splitArray.length != 8) {
            throw new Exception("Wrong input");
        }

        String nationKey = splitArray[3];
        // Count how many entries from this nation there are
        BigDecimal nationCount = new BigDecimal("0");
        double curr = 0D;
        String accountBalance = splitArray[5];
        if (nationToSumCount.containsKey(nationKey)) {
            nationCount = nationToSumCount.get(nationKey).getValue1();
            curr = nationToSumCount.get(nationKey).getValue0();
        }
        nationCount = nationCount.add(new BigDecimal("1"));
        curr += Double.parseDouble(accountBalance);
        nationToSumCount.put(nationKey, new Pair<Double, BigDecimal>(curr, nationCount));

    }
}
