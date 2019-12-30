package reactor.aeron;

import org.agrona.CloseHelper;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AeronEventLoop implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(AeronEventLoop.class);

  private final DynamicCompositeAgent agent;
  private final AgentRunner agentRunner;

  /**
   * Constructor.
   *
   * @param name agent name
   * @param workerId worker id
   * @param groupId id of parent {@link AeronEventLoopGroup}
   * @param idleStrategy {@link IdleStrategy} instance for this event loop
   */
  AeronEventLoop(String name, int workerId, int groupId, IdleStrategy idleStrategy) {
    this(String.format("%s-%x-%d", name, groupId, workerId), idleStrategy);
  }

  /**
   * Constructor.
   *
   * @param name agent name
   * @param idleStrategy {@link IdleStrategy} instance for this event loop
   */
  public AeronEventLoop(String name, IdleStrategy idleStrategy) {
    agent = new DynamicCompositeAgent(name);
    agentRunner =
        new AgentRunner(idleStrategy, th -> logger.error("Exception occurred: ", th), null, agent);
    AgentRunner.startOnThread(agentRunner);
  }

  /**
   * Registers agent in event loop.
   *
   * @param resource aeron resource
   */
  public void register(Agent resource) {
    agent.add(resource);
  }

  @Override
  public void close() {
    CloseHelper.quietClose(agentRunner);
  }
}
