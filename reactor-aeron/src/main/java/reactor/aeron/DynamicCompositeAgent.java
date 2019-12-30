package reactor.aeron;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

/**
 * Group several {@link Agent}s into one composite so they can be scheduled as a unit.
 *
 * <p>{@link Agent}s can be dynamically added and removed.
 *
 * <p><b>Note:</b> This class is thread-safe for add and remove.
 */
public class DynamicCompositeAgent implements Agent {

  public enum Status {
    /** Agent is being initialised and has not yet been started. */
    INIT,

    /** Agent is not active after a successful <code>{@link #onStart()}</code>. */
    ACTIVE,

    /** Agent has been closed. */
    CLOSED
  }

  private final String roleName;

  private final Queue<Runnable> commands = new ManyToOneConcurrentLinkedQueue<>();
  private final ArrayList<Agent> agents;

  private volatile Status status = Status.INIT;

  /**
   * Construct a new composite that has no {@link Agent}s to begin with.
   *
   * @param roleName to be given for {@link Agent#roleName()}.
   */
  public DynamicCompositeAgent(String roleName) {
    this.roleName = roleName;
    this.agents = new ArrayList<>();
  }

  /**
   * Construct a new composite with collection of {@link Agent}s.
   *
   * @param roleName to be given for {@link Agent#roleName()}.
   * @param agents the parts of this composite, at least one agent and no null agents allowed
   * @throws NullPointerException if the array or any element is null
   */
  public DynamicCompositeAgent(String roleName, Collection<? extends Agent> agents) {
    this.roleName = roleName;
    this.agents = new ArrayList<>(agents.size());
    for (Agent agent : agents) {
      Objects.requireNonNull(agent, "agent cannot be null");
      this.agents.add(agent);
    }
  }

  /**
   * Construct a new composite with one or several {@link Agent}s.
   *
   * @param roleName to be given for {@link Agent#roleName()}.
   * @param agents the parts of this composite, at least one agent and no null agents allowed
   * @throws NullPointerException if the array or any element is null
   */
  public DynamicCompositeAgent(String roleName, Agent... agents) {
    this.roleName = roleName;
    this.agents = new ArrayList<>(agents.length);
    for (Agent agent : agents) {
      Objects.requireNonNull(agent, "agent cannot be null");
      this.agents.add(agent);
    }
  }

  /**
   * Get the {@link Status} for the Agent.
   *
   * @return the {@link Status} for the Agent.
   */
  public Status status() {
    return status;
  }

  @Override
  public void onStart() {
    for (Agent agent : agents) {
      agent.onStart();
    }

    status = Status.ACTIVE;
  }

  @Override
  public int doWork() {
    int workCount = 0;

    processCommands();

    ArrayList<Agent> agents = this.agents;
    for (int lastIndex = agents.size() - 1, i = lastIndex; i >= 0; i--) {
      Agent agent = agents.get(i);
      try {
        int result = agent.doWork();
        if (result > 0) {
          workCount += result;
        }
        if (result < 0) {
          ArrayListUtil.fastUnorderedRemove(agents, i, lastIndex--);
          safetyClose(agent);
        }
      } catch (Throwable th) {
        ArrayListUtil.fastUnorderedRemove(agents, i, lastIndex--);
        safetyClose(agent);
      }
    }

    return workCount;
  }

  @Override
  public void onClose() {
    status = Status.CLOSED;

    processCommands();

    agents.forEach(this::safetyClose);
    agents.clear();
  }

  @Override
  public String roleName() {
    return roleName;
  }

  /**
   * Add a new {@link Agent} to the composite.
   *
   * <p>The agent will be added during the next invocation of {@link #doWork()} if this operation is
   * successful. If the {@link Agent#onStart()} method throws an exception then it will not be added
   * and {@link Agent#onClose()} will be called.
   *
   * @param agent to be added to the composite.
   */
  public void add(Agent agent) {
    Objects.requireNonNull(agent, "agent cannot be null");
    if (Status.ACTIVE != status) {
      throw new IllegalStateException("add called when not active");
    }
    commands.add(() -> add0(agent));
  }

  /**
   * Remove an {@link Agent} from the composite.
   *
   * <p>The agent is removed during the next {@link #doWork()} duty cycle if this operation is
   * successful. The {@link Agent} is removed by identity. Only the first found is removed.
   *
   * @param agent to be removed.
   */
  public void remove(Agent agent) {
    Objects.requireNonNull(agent, "agent cannot be null");
    if (Status.ACTIVE != status) {
      throw new IllegalStateException("remove called when not active");
    }
    commands.add(() -> remove0(agent));
  }

  private void add0(Agent agent) {
    if (Status.ACTIVE != status) {
      safetyClose(agent);
      return;
    }
    try {
      agent.onStart();
      agents.add(agent);
    } catch (Throwable th) {
      safetyClose(agent);
    }
  }

  private void remove0(Agent agent) {
    if (Status.ACTIVE != status) {
      return;
    }
    if (ArrayListUtil.fastUnorderedRemove(agents, agent)) {
      safetyClose(agent);
    }
  }

  private void safetyClose(Agent agent) {
    try {
      agent.onClose();
    } catch (Throwable ignored) {
      // ignore
    }
  }

  private void processCommands() {
    for (Runnable action = commands.poll(); action != null; action = commands.poll()) {
      action.run();
    }
  }
}
