package homework2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;


public class RpcClient
{

    public static void main(String[] args) throws IOException {
        //获取服务端代理
        IProxyProtocol client = RPC.getProxy(IProxyProtocol.class, IProxyProtocol.versionID, new InetSocketAddress("localhost", 8888), new Configuration());
        System.out.println("------------------- client start");
        //调用服务端的创建方法。
        String studentId = "G20210735010393";
        String name = client.findName(studentId);
        System.out.format("-----student:%-20s----------name:%-5s",studentId,name);

    }

}