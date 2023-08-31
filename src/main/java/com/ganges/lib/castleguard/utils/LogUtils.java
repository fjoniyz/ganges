package com.ganges.lib.castleguard.utils;

import com.ganges.lib.castleguard.CastleGuard;
import java.util.HashMap;
import org.apache.commons.lang3.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {

  private static final Logger logger = LoggerFactory.getLogger(CastleGuard.class);

  public static void logGlobalRanges(HashMap<String, Range<Double>> ranges) {
    logger.info("Global range: ");
    for (String header : ranges.keySet()) {
      logger.info(header + ": " + ranges.get(header).toString() + ", ");
    }
  }
}
