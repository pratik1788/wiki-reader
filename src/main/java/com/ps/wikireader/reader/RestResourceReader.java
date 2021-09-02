package com.ps.wikireader.reader;

import com.ps.wikireader.pojo.WikiData;
import com.ps.wikireader.pojo.WikiDataCollection;
import com.ps.wikireader.producer.DataProducer;
import com.ps.wikireader.util.FileNameConverterUtil;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.zip.GZIPInputStream;

@Service
@Setter
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

    @Value("${application.data.batch-size}")
    private int batchSize;

    public boolean extractAndProcessData(String fileName) {
        logger.info("import data request received with parameter as {}",fileName);
        RequestCallback requestCallback = request -> request
                .getHeaders()
                .setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM, MediaType.ALL));
        int yearMonthDay= FileNameConverterUtil.getYearMonthDayFromFileName(fileName);
        int hour= FileNameConverterUtil.getHourFileName(fileName);
        Boolean readSuccessFlag= restTemplate.execute(baseUrl+FileNameConverterUtil.getFilePathFromFileName(fileName), HttpMethod.GET, requestCallback, clientHttpResponse -> {
            int rowCount=1;
            InputStream gzipStream = new GZIPInputStream(clientHttpResponse.getBody());
            Reader decoder = new InputStreamReader(gzipStream);
            BufferedReader buffered = new BufferedReader(decoder);
            String content;
            logger.info("chunk received");
            dataProducer.sendMessage(WikiDataCollection.newBuilder().setWikiDataList(Collections.emptyList()).build(),"START"+"~"+fileName);
            List<WikiData> wikiDataList= new ArrayList<>();
            while ((content = buffered.readLine()) != null && (readlimit == -1 || rowCount <= readlimit)) {
                String[] data=content.split(" ");
                wikiDataList.add(WikiData.newBuilder().setYearMonthDay(yearMonthDay)
                        .setHourOfDay(hour)
                        .setLanguage(data[0])
                        .setPageName(data[1])
                        .setNonUniqueViews(Integer.parseInt(data[2]))
                        .setRecordId(rowCount)
                        .setBytesTransferred((Long.parseLong(data[3]))).build());
                if(rowCount%batchSize == 0) {
                    dataProducer.sendMessage(WikiDataCollection.newBuilder().setWikiDataList(wikiDataList).build(), "DATA" + "~" + fileName + "~" + rowCount);
                    wikiDataList= new ArrayList<>();
                }
                rowCount++;
            }
            if(rowCount%batchSize!=0){
                dataProducer.sendMessage(WikiDataCollection.newBuilder().setWikiDataList(wikiDataList).build(), "DATA" + "~" + fileName + "~" + rowCount);
                wikiDataList=null;
            }
            dataProducer.sendMessage(WikiDataCollection.newBuilder().setWikiDataList(Collections.emptyList()).build(),"END"+"~"+fileName);
            logger.info("Published {} no. of message",rowCount);
            return true;
        });
        return readSuccessFlag;
    }



}
