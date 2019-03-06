package reactor.aeron;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseAeronTest {

  public static final Logger logger = LoggerFactory.getLogger(BaseAeronTest.class);

  public static final Duration TIMEOUT = Duration.ofSeconds(30);

  @BeforeEach
  public final void baseSetUp(TestInfo testInfo) {
    logger.info("***** Test started  : " + testInfo.getDisplayName() + " *****");
  }

  @AfterEach
  public final void baseTearDown(TestInfo testInfo) {
    logger.info("***** Test finished : " + testInfo.getDisplayName() + " *****");
  }
}
