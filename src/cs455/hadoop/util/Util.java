package cs455.hadoop.util;

/**
 * Author: Thilina
 * Date: 4/22/14
 */
public class Util {

    public static int getPublishedYearFromFileName(String fileName) {
        String publishedYearStr = fileName.split("[0-9]*-Year")[1].split(".txt")[0];
        int publishedYear;
        if(publishedYearStr.toUpperCase().contains("BC")){
            publishedYearStr = publishedYearStr.replaceFirst("BC","");
            // For books published in BC, multiply the number by -1
            publishedYear = Integer.parseInt(publishedYearStr) * -1;
        } else {
            publishedYear = Integer.parseInt(publishedYearStr);
        }
        return publishedYear;
    }

    public static int getDecadeFromYear(int publishedYear) {
        return publishedYear - (publishedYear % 10);
    }
}
