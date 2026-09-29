package vn.huyqt.logbroker.controller;
import java.util.*;
/** Strict, shared option and fixed-voter parsing without a CLI framework. */
public final class ControllerOptions {
    private ControllerOptions(){}
    public static Map<String,String> parse(String[] args,int start,Set<String> allowed){var values=new LinkedHashMap<String,String>();for(int i=start;i<args.length;i+=2){String key=args[i];if(!key.startsWith("--")||!allowed.contains(key.substring(2))||i+1>=args.length||args[i+1].startsWith("--"))throw new IllegalArgumentException("Unknown or missing option: "+key);if(values.putIfAbsent(key.substring(2),args[i+1])!=null)throw new IllegalArgumentException("Duplicate option: "+key);}return values;}
    public static String required(Map<String,String> options,String key){var value=options.get(key);if(value==null||value.isBlank())throw new IllegalArgumentException("Missing --"+key);return value;}
    public static ClusterIdentity identity(String cluster,int node,String text){var voters=new ArrayList<ClusterIdentity.Voter>();for(String token:text.split(",",-1)){int at=token.indexOf('@'),colon=token.lastIndexOf(':');if(at<1||colon<=at+1)throw new IllegalArgumentException("Voters must be id@host:port, exactly three");String host=token.substring(at+1,colon);if(host.startsWith("[")&&host.endsWith("]"))host=host.substring(1,host.length()-1);voters.add(new ClusterIdentity.Voter(Integer.parseInt(token.substring(0,at)),host,Integer.parseInt(token.substring(colon+1))));}return new ClusterIdentity(UUID.fromString(cluster),node,voters);}
}
