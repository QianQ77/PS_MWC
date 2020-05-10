package mwcp.spark;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Created by qiuqian on 4/4/18.
 */
public class ZKTest implements Watcher, Serializable {
    private String znode = null;
    private static ZooKeeper zk;
    private static final String ZOOKEEPER_SERVER_ADDRESS = "localhost:2181";
    private static final String ZK_MWCL = "MWCL";

    public ZKTest() {
        String zkServerAddress = ZOOKEEPER_SERVER_ADDRESS;
        if (zk == null) {
            System.out.println("Starting ZK:");
            try {
                zk = new ZooKeeper(ZOOKEEPER_SERVER_ADDRESS, 48 * 60 * 60 * 1000, this);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            znode = "/" + ZK_MWCL;
            // clean existing znode from previous runs
            if (zk.exists(znode, false) != null) {
                zk.delete(znode, -1);
            }
            System.out.println("Creating znode: " + znode);
            byte[] initialValue = new byte[4]; // int(0)
            zk.create(znode, initialValue, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int readFromZookeeper() {
        try {
            byte[] val = zk.getData(znode, this, null);
            return ByteBuffer.wrap(val).getInt();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void process(WatchedEvent event) {
        // Nothing. Overhead of watching the zk server should be minimal in
        // the spark driver class
    }

    public static void main(String[] args){

        ZKTest test = new ZKTest();
        int valueSoFar = test.readFromZookeeper();

        int newValue = 300;

        if(newValue > valueSoFar) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(newValue);
            try {
                zk.setData(test.znode, buf.array(), -1);
                System.out.println("Updated znode: " + test.readFromZookeeper());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
