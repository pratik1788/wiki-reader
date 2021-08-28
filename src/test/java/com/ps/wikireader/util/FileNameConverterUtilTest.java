package com.ps.wikireader.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FileNameConverterUtilTest {

    @Test
    void getYearMonthDayFromFileName() {
        Assertions.assertEquals(20120101,FileNameConverterUtil.getYearMonthDayFromFileName("pagecounts-20120101-000000.gz") );
    }

    @Test
    void getHourFileName() {
        Assertions.assertEquals(0,FileNameConverterUtil.getHourFileName("pagecounts-20120101-000000.gz") );
    }

    @Test
    void getFilePathFromFileName() {
        Assertions.assertEquals("2012/2012-01/pagecounts-20120101-000000.gz",FileNameConverterUtil.getFilePathFromFileName("pagecounts-20120101-000000.gz") );
    }
}