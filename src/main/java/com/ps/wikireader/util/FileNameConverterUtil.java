package com.ps.wikireader.util;

public class FileNameConverterUtil {
    public static int getYearMonthDayFromFileName(String fileName){
        String[] fileNameSplit=fileName.split("-");
        return Integer.parseInt(fileNameSplit[1]);
    }
    public static int getHourFileName(String fileName){
        String[] fileNameSplit=fileName.split("-");
        return Integer.parseInt(fileNameSplit[2].split("\\.")[0]);
    }
    public static String getFilePathFromFileName(String fileName){
        String[] fileNameSplit=fileName.split("-");
        String year= fileNameSplit[1].substring(0,4);
        String yearMonth= fileNameSplit[1].substring(0,4)+"-"+fileNameSplit[1].substring(4,6);
        return year + "/" +
                yearMonth + "/" +
                fileName;
    }
}
