package homework2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;

import java.io.IOException;
import java.util.HashMap;

public class RpcServer implements IProxyProtocol {

   static HashMap<String,String> map = new HashMap<String, String>(){{put("G20210735010393", "phy");}};

    public int Add(int number1, int number2) {
        return number1+number2;
    }

    public String findName(String studentId) {
           //{{ map.put("G20210735010393", "phy"); }};
        System.out.format("--------student:%-20s",studentId);
        return map.get(studentId);
    }

    public long getProtocolVersion(String s, long l) throws IOException {
        return IProxyProtocol.versionID;
    }

    public ProtocolSignature getProtocolSignature(String s, long l, int i) throws IOException {
        return null;
    }
    public static void main(String[] args) throws IOException {
        //创建RPC 服务

        Server server = new RPC.Builder(new Configuration()).setBindAddress("localhost").setPort(8888)
                .setProtocol(IProxyProtocol.class).setInstance(new RpcServer()).build();
        System.out.println("服务器开始工作");
        //启动服务
        server.start();
    }

}
