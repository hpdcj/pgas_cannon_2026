package cannon.cluster.actors;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.*;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import cannon.cluster.CborSerializable;
import cannon.cluster.other.IterationMatrix;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class ConnectionActor extends AbstractBehavior<ConnectionActor.Command> {

    public static final ServiceKey<Command> SERVICE_KEY = ServiceKey.create(Command.class, "ConnectionActor");

    public interface Command extends CborSerializable {}

    private record WorkersUpdated(Set<ActorRef<Command>> newWorkers) implements Command {}
    private static final class Synchronize implements Command {}
    private record Ready(int nodeId) implements Command {}
    public record SendAMatrixToNode(IterationMatrix matrix, int nodeId) implements Command {}
    public record SendBMatrixToNode(IterationMatrix matrix, int nodeId) implements Command {}
    public record ReceiveAMatrix(IterationMatrix matrix) implements Command {}
    public record ReceiveBMatrix(IterationMatrix matrix) implements Command {}

    private final int totalWorkers;
    private final List<ActorRef<Command>> workers = new ArrayList<>();
    private final List<ActorRef<Command>> workersAddresses = new ArrayList<>();
    private final ActorRef<CannonActor.Command> parent;
    private final ActorRef<Receptionist.Listing> subscriptionAdapter;
    private int synchronizationCounter = 0;

    private ConnectionActor(ActorContext<Command> context, ActorRef<CannonActor.Command> parent, int totalWorkers) {
        super(context);
        this.parent = parent;
        this.totalWorkers = totalWorkers;
        this.subscriptionAdapter = context.messageAdapter(Receptionist.Listing.class, listing ->
                new WorkersUpdated(listing.getServiceInstances(SERVICE_KEY)));
        context.getSystem().receptionist().tell(Receptionist.subscribe(SERVICE_KEY, subscriptionAdapter));
    }

    public static Behavior<Command> create(ActorRef<CannonActor.Command> parent, int totalWorkers) {
        return Behaviors.setup(context -> {
            context.getSystem().receptionist().tell(Receptionist.register(SERVICE_KEY, context.getSelf().narrow()));

            return new ConnectionActor(context, parent, totalWorkers);
        });
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
            .onMessage(WorkersUpdated.class, this::onWorkersUpdated)
            .onMessage(Synchronize.class, this::onSynchronize)
            .onMessage(Ready.class, this::onReady)
            .onMessage(SendAMatrixToNode.class, this::onSendAMatrixToNode)
            .onMessage(SendBMatrixToNode.class, this::onSendBMatrixToNode)
            .onMessage(ReceiveAMatrix.class, this::onReceiveAMatrix)
            .onMessage(ReceiveBMatrix.class, this::onReceiveBMatrix)
            .build();
    }

    private Behavior<Command> onWorkersUpdated(WorkersUpdated event) {
        workers.clear();
        workers.addAll(event.newWorkers);

        if (workers.size() == totalWorkers) {
            workers.sort(Comparator.comparing(worker -> worker.path().parent().toStringWithoutAddress()));
            workersAddresses.addAll(workers);

            for (ActorRef<Command> worker : workersAddresses) {
                worker.tell(new Synchronize());
            }
        }

        return Behaviors.same();
    }

    private Behavior<Command> onSynchronize(Synchronize message) {
        synchronizationCounter++;

        if (synchronizationCounter == totalWorkers) {
            int myIndex = -1;
            String myPath = getContext().getSelf().path().parent().toStringWithoutAddress();

            for (int i = 0; i < workersAddresses.size(); i++) {
                if (workersAddresses.get(i).path().parent().toStringWithoutAddress().equals(myPath)) {
                    myIndex = i;

                    break;
                }
            }

            getContext().getSelf().tell(new Ready(myIndex));
        }

        return Behaviors.same();
    }

    private Behavior<Command> onReady(Ready message) {
        parent.tell(new CannonActor.StartAlgorithm(getContext().getSelf(), message.nodeId));

        return Behaviors.same();
    }

    private Behavior<Command> onSendAMatrixToNode(SendAMatrixToNode event) {
        workersAddresses.get(event.nodeId).tell(new ReceiveAMatrix(event.matrix));

        return Behaviors.same();
    }

    private Behavior<Command> onSendBMatrixToNode(SendBMatrixToNode event) {
        workersAddresses.get(event.nodeId).tell(new ReceiveBMatrix(event.matrix));

        return Behaviors.same();
    }

    private Behavior<Command> onReceiveAMatrix(ReceiveAMatrix event) {
        parent.tell(new CannonActor.AddAMatrixToQueue(event.matrix));

        return Behaviors.same();
    }

    private Behavior<Command> onReceiveBMatrix(ReceiveBMatrix event) {
        parent.tell(new CannonActor.AddBMatrixToQueue(event.matrix));

        return Behaviors.same();
    }
}
