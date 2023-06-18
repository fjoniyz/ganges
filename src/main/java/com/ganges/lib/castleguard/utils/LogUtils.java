package com.ganges.lib.castleguard.utils;

import com.ganges.lib.castleguard.CastleGuard;
import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class LogUtils {

  private static final Logger logger = LoggerFactory.getLogger(CastleGuard.class);

  public static void logGlobalRanges(HashMap<String, Range<Float>> ranges) {
    logger.info("Global range: ");
    for (String header : ranges.keySet()) {
      logger.info(header + ": " + ranges.get(header).toString() + ", ");
    }
  }
}
