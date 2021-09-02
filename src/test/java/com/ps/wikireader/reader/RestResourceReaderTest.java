package com.ps.wikireader.reader;

import com.ps.wikireader.pojo.WikiDataCollection;
import com.ps.wikireader.producer.DataProducer;
import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.client.MockClientHttpResponse;
import org.springframework.web.client.RequestCallback;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestTemplate;

import java.io.InputStream;
import java.util.List;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RestResourceReaderTest {

    @InjectMocks
    private RestResourceReader restResourceReader;

    @Mock
    private RestTemplate restTemplate;

    @Mock
    private DataProducer dataProducer;

    @Captor
    private ArgumentCaptor<ResponseExtractor<Boolean>> argumentCaptor;

    @Test
    void extractAndProcessData_retruns_true() {
            Mockito.when(restTemplate.execute(anyString()
                ,any(HttpMethod.class), any(RequestCallback.class),
                any(ResponseExtractor.class))).thenReturn(true);
        Assertions.assertTrue(restResourceReader.extractAndProcessData("pagecounts-20120101-000000.gz"));

    }
    @Test
    void extractAndProcessData_retruns_false() {
        Mockito.when(restTemplate.execute(anyString()
                ,any(HttpMethod.class), any(RequestCallback.class),
                any(ResponseExtractor.class))).thenReturn(false);
        Assertions.assertFalse(restResourceReader.extractAndProcessData("pagecounts-20120101-000000.gz"));

    }

    @Test
    void extractAndProcessData_test_batchSize_1() throws Exception{
        Mockito.when(restTemplate.execute(anyString(),any(HttpMethod.class), any(RequestCallback.class),
                any(ResponseExtractor.class))).thenReturn(true);
        setBatchSizeAndReadLimit(5,-1);
        restResourceReader.extractAndProcessData("pagecounts-20120101-000000.gz");
        verify(restTemplate).execute(anyString(), any(HttpMethod.class),any(RequestCallback.class),argumentCaptor.capture());
        ResponseExtractor<Boolean> extractDataCallback = argumentCaptor.getValue();
        InputStream stream = RestResourceReaderTest.class.getResourceAsStream("/TestFile.gz");
        MockClientHttpResponse response = new MockClientHttpResponse(stream, HttpStatus.OK);
        extractDataCallback.extractData(response);
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),eq("START"+"~"+"pagecounts-20120101-000000.gz"));
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),eq("END"+"~"+"pagecounts-20120101-000000.gz"));
        Mockito.verify(dataProducer,times(5)).sendMessage(any(WikiDataCollection.class),contains("DATA"));
    }
    @Test
    void extractAndProcessData_test_batchSize_2() throws Exception{
        setBatchSizeAndReadLimit(1,-1);
        Mockito.when(restTemplate.execute(anyString(),any(HttpMethod.class), any(RequestCallback.class),
                any(ResponseExtractor.class))).thenReturn(true);
        restResourceReader.extractAndProcessData("pagecounts-20120101-000000.gz");
        verify(restTemplate).execute(anyString(), any(HttpMethod.class),any(RequestCallback.class),argumentCaptor.capture());
        ResponseExtractor<Boolean> extractDataCallback = argumentCaptor.getValue();
        InputStream stream = RestResourceReaderTest.class.getResourceAsStream("/TestFile.gz");
        MockClientHttpResponse response = new MockClientHttpResponse(stream, HttpStatus.OK);
        extractDataCallback.extractData(response);
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),eq("START"+"~"+"pagecounts-20120101-000000.gz"));
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),eq("END"+"~"+"pagecounts-20120101-000000.gz"));
        Mockito.verify(dataProducer,times(23)).sendMessage(any(WikiDataCollection.class),contains("DATA"));
    }

    @Test
    void extractAndProcessData_test_batchSize_3() throws Exception{
        setBatchSizeAndReadLimit(100,-1);
        Mockito.when(restTemplate.execute(anyString(),any(HttpMethod.class), any(RequestCallback.class),
                any(ResponseExtractor.class))).thenReturn(true);
        restResourceReader.extractAndProcessData("pagecounts-20120101-000000.gz");
        verify(restTemplate).execute(anyString(), any(HttpMethod.class),any(RequestCallback.class),argumentCaptor.capture());
        ResponseExtractor<Boolean> extractDataCallback = argumentCaptor.getValue();
        InputStream stream = RestResourceReaderTest.class.getResourceAsStream("/TestFile.gz");
        MockClientHttpResponse response = new MockClientHttpResponse(stream, HttpStatus.OK);
        extractDataCallback.extractData(response);
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),eq("START"+"~"+"pagecounts-20120101-000000.gz"));
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),eq("END"+"~"+"pagecounts-20120101-000000.gz"));
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),contains("DATA"));
    }

    @Test
    void extractAndProcessData_test_readLimit() throws Exception{
        setBatchSizeAndReadLimit(1,5);
        Mockito.when(restTemplate.execute(anyString(),any(HttpMethod.class), any(RequestCallback.class),
                any(ResponseExtractor.class))).thenReturn(true);
        restResourceReader.extractAndProcessData("pagecounts-20120101-000000.gz");
        verify(restTemplate).execute(anyString(), any(HttpMethod.class),any(RequestCallback.class),argumentCaptor.capture());
        ResponseExtractor<Boolean> extractDataCallback = argumentCaptor.getValue();
        InputStream stream = RestResourceReaderTest.class.getResourceAsStream("/TestFile.gz");
        MockClientHttpResponse response = new MockClientHttpResponse(stream, HttpStatus.OK);
        extractDataCallback.extractData(response);
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),eq("START"+"~"+"pagecounts-20120101-000000.gz"));
        Mockito.verify(dataProducer,times(1)).sendMessage(any(WikiDataCollection.class),eq("END"+"~"+"pagecounts-20120101-000000.gz"));
        Mockito.verify(dataProducer,times(5)).sendMessage(any(WikiDataCollection.class),contains("DATA"));
    }

    private void setBatchSizeAndReadLimit(int batchSize,int readLimit){
        restResourceReader.setBatchSize(batchSize);
        restResourceReader.setReadlimit(readLimit);
    }


}