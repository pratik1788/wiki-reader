package com.ps.wikireader.reader;

import com.ps.wikireader.pojo.WikiData;
import com.ps.wikireader.producer.DataProducer;
import com.ps.wikireader.util.FileNameConverterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

@Service
public class RestResourceReader {

    private static final Logger logger = LoggerFactory.getLogger(RestResourceReader.class);

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private DataProducer dataProducer;

    @Value("${application.data.extraction.base-url}")
    private String baseUrl;

    @Value("${application.data.read-limit}")
    private int readlimit;

    public boolean extractAndProcessData(String fileName) {
        logger.info("import data request received with parameter as {}",fileName);
        RequestCallback requestCallback = request -> request
                .getHeaders()
                .setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM, MediaType.ALL));
        int yearMonthDay= FileNameConverterUtil.getYearMonthDayFromFileName(fileName);
        int hour= FileNameConverterUtil.getHourFileName(fileName);
        Boolean readSuccessFlag= restTemplate.execute(baseUrl+FileNameConverterUtil.getFilePathFromFileName(fileName), HttpMethod.GET, requestCallback, clientHttpResponse -> {
            int rowCount=0;
            InputStream gzipStream = new GZIPInputStream(clientHttpResponse.getBody());
            Reader decoder = new InputStreamReader(gzipStream);
            BufferedReader buffered = new BufferedReader(decoder);
            String content;
            logger.info("chunk received");
            while ((content = buffered.readLine()) != null && readlimit != -1 && rowCount < readlimit) {
                String[] data=content.split(" ");
                dataProducer.sendMessage(WikiData.newBuilder().setYearDateDay(yearMonthDay)
                        .setHourOfDay(hour)
                        .setLanguage(data[0])
                        .setPageName(data[1])
                        .setNonUniqueViews(Integer.parseInt(data[2]))
                        .setBytesTransferred((Long.parseLong(data[3]))).build(),fileName+"~"+rowCount);
                rowCount++;
            }
            logger.info("Published {} no. of message",rowCount);
            return true;
        });
        return readSuccessFlag != null;
    }



}
