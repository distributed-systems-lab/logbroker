package vn.huyqt.logbroker.controller.support;
import java.util.*;import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;
/** Independent public-call observations; no commit offsets enter the abstract model. */
public sealed interface HistoryEvent {
    record Invocation(long id,Command command,long start)implements HistoryEvent{}
    record Completion(long id,Result result,long end)implements HistoryEvent{}
    record Crash(int node)implements HistoryEvent{}
    record Delivery(int source,int target,long requestId)implements HistoryEvent{}
    sealed interface Command{ }
    record Create(String name,int partitions)implements Command{}
    record Read()implements Command{}
    sealed interface Result{}
    record Created(UUID id)implements Result{}
    record Topics(List<TopicCreated> values)implements Result{public Topics{values=List.copyOf(values);}}
    record Rejected(vn.huyqt.logbroker.controller.protocol.QuorumError error)implements Result{}
    record Unknown()implements Result{}
}
