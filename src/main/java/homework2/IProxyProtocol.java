package homework2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.VersionedProtocol;


public interface IProxyProtocol extends VersionedProtocol {
    static final long versionID = 1L; //版本号，默认情况下，不同版本号的RPC Client和Server之间不能相互通信
    int Add(int number1,int number2);
    String findName(String studentId);


}