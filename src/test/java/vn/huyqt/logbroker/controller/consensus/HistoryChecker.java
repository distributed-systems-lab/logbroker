package vn.huyqt.logbroker.controller.consensus;
import java.util.*;import vn.huyqt.logbroker.controller.support.HistoryEvent;import vn.huyqt.logbroker.controller.support.HistoryEvent.*;import vn.huyqt.logbroker.broker.metadata.TopicCatalog.TopicCreated;import vn.huyqt.logbroker.controller.protocol.QuorumError;
/** Exhaustive bounded serial-order search respecting invocation/completion real-time edges. */
final class HistoryChecker {
    private record Op(Invocation invocation,Completion completion){}
    static boolean check(List<HistoryEvent> history){var starts=new LinkedHashMap<Long,Invocation>();var ends=new HashMap<Long,Completion>();var observed=new HashMap<String,TopicCreated>();
        for(var event:history){if(event instanceof Invocation start)starts.put(start.id(),start);if(event instanceof Completion end){ends.put(end.id(),end);if(end.result() instanceof Topics topics)for(var topic:topics.values())observed.putIfAbsent(topic.name(),topic);}}
        if(starts.size()>8)throw new IllegalArgumentException("History exceeds bounded search (8 operations)");var ops=starts.values().stream().map(start->new Op(start,ends.get(start.id()))).toList();return search(ops,new boolean[ops.size()],new HashMap<>(),observed,0);
    }
    private static boolean search(List<Op> ops,boolean[] done,Map<String,TopicCreated> state,Map<String,TopicCreated> observed,int count){if(count==ops.size())return true;
        for(int i=0;i<ops.size();i++){if(done[i])continue;Op op=ops.get(i);boolean blocked=false;for(int j=0;j<ops.size();j++){if(!done[j]&&j!=i&&ops.get(j).completion()!=null&&!(ops.get(j).completion().result() instanceof Unknown)&&ops.get(j).completion().end()<op.invocation().start()){blocked=true;break;}}if(blocked)continue;
            done[i]=true;var next=new HashMap<>(state);var result=op.completion()==null?new Unknown():op.completion().result();
            if(result instanceof Unknown){if(search(ops,done,state,observed,count+1)){done[i]=false;return true;}if(op.invocation().command() instanceof Create create&&!next.containsKey(create.name())){var topic=observed.getOrDefault(create.name(),new TopicCreated(UUID.nameUUIDFromBytes(create.name().getBytes(java.nio.charset.StandardCharsets.UTF_8)),create.name(),create.partitions()));if(topic.partitions()==create.partitions())next.put(create.name(),topic);else{done[i]=false;continue;}}else{done[i]=false;continue;}}
            else if(op.invocation().command() instanceof Create create){var old=next.get(create.name());if(result instanceof Created created){if(old!=null&&(!old.id().equals(created.id())||old.partitions()!=create.partitions())){done[i]=false;continue;}next.put(create.name(),new TopicCreated(created.id(),create.name(),create.partitions()));}else if(result instanceof Rejected rejected){if(rejected.error()==QuorumError.TOPIC_ALREADY_EXISTS&&(old==null||old.partitions()==create.partitions())){done[i]=false;continue;}}else{done[i]=false;continue;}}
            else if(result instanceof Topics topics){var actual=new HashMap<String,TopicCreated>();for(var topic:topics.values())actual.put(topic.name(),topic);if(!actual.equals(state)){done[i]=false;continue;}}
            else if(!(result instanceof Rejected)){done[i]=false;continue;}
            if(search(ops,done,next,observed,count+1)){done[i]=false;return true;}done[i]=false;
        }return false;
    }
}

