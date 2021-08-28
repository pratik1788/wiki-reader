package com.ps.wikireader.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public enum EvenetName {
    REQUEST_TO_START_READING_RESOURCE ("requestToStartReadingResource"),
    RESOURCE_READING_STARTED("resourceReadingStarted"),
    RESOURCE_READING_FAILED("resourceReadingFailed"),
    RESOURCE_READING_SUCCESSFUL("resourceReadingSuccessful"),
    DATA_SINK_STARTED("dataSinkStarted"),
    DATA_SINK_IN_PROGRESS("dataSinkInProgress"),
    DATA_SINK_SUCCESSFUL("dataSinkSuccessful"),
    DATA_SINK_FAILED("dataSinkFailed");
    String name;
}
