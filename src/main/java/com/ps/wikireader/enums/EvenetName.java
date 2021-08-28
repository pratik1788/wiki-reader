package com.ps.wikireader.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public enum EvenetName {
    START_EXTRACTION ("startExtraction"),
    EXTRATCION_STARTED ("extrationStarted"),
    EXTRATCION_FAILED("extractionFailed"),
    EXTRATCION_SUCCESSFUL("extractionSuccessful"),
    CONSUMPTION_STARTED("consumptionStarted"),
    CONSUMPTION_SUCCESSFUL("consumptionSuccessful"),
    CONSUMPTION_FAILED("consumptionFailed");

    String name;
}
