package com.ps.wikireader.controller;


import com.ps.wikireader.pojo.ExtractionRequest;
import com.ps.wikireader.reader.RestResourceReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController("/api/v1")
public class ExtractionController {

    @Autowired
    private RestResourceReader restResourceReader;

    @RequestMapping(value = "/extractData", method = RequestMethod.POST)
    public String extract(@RequestBody ExtractionRequest extractionRequest) throws IOException {
        return restResourceReader.extractAndProcessData(extractionRequest)?"Success":"Failure";
    }
}
