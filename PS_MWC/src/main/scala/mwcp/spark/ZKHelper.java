package mwcp.spark;

import org.apache.commons.lang.SerializationUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by qiuqian on 4/4/18.
 */
public class ZKHelper {
    private static final String znode_weight = "/MWCL";
    private static final String znode_vertices = "/MWCLV";

    /**
     * Be called on both driver node and worker nodes
     * @param watcher
     * @return
     */
    public static ZooKeeper startZK(Watcher watcher, String zkServerIp) {
        System.out.println("Starting ZK:");
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(zkServerIp.concat(":2181"), 48 * 60 * 60 * 1000, watcher);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return zk;
    }

    /**
     * Be called on driver node
     * @param zk
     */
    public static void initializeZNode(ZooKeeper zk, double initial_value) {
        try {
            // clean existing znode_weight from previous runs
            if (zk.exists(znode_weight, false) != null) {
                zk.delete(znode_weight, -1);
            }
            System.out.println("Creating znode_weight: " + znode_weight);
            byte[] initialValue = ByteBuffer.allocate(8).putDouble(initial_value).array();
            zk.create(znode_weight, initialValue, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Deprecated. Test on web-uk-2005; cost 279s.
     * Be called on driver node
     * @param zk
     */
    public static void initializeZNodeVertices(ZooKeeper zk, double initial_value, byte[] vertices_byte) {
        try {
            // clean existing znode_weight from previous runs
            if (zk.exists(znode_weight, false) != null) {
                zk.delete(znode_weight, -1);
            }
            // clean existing znode_vertices from previous runs
            if (zk.exists(znode_vertices, false) != null) {
                zk.delete(znode_vertices, -1);
            }

            System.out.println("Creating znode_weight: " + znode_weight);
            byte[] initialValue = ByteBuffer.allocate(8).putDouble(initial_value).array();

            zk.create(znode_weight, initialValue, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            System.out.println("Creating znode_vertices: " + znode_vertices);
            zk.create(znode_vertices, vertices_byte, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static double readWeightFromZookeeper(ZooKeeper zk, Watcher watcher) {
        try {
            byte[] val = zk.getData(znode_weight, watcher, null);
            return ByteBuffer.wrap(val).getDouble();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static byte[] readVerticesFromZookeeper(ZooKeeper zk, Watcher watcher) {
        try {
            byte[] val = zk.getData(znode_vertices, watcher, null);
            return val;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public static double updateZookeeper(ZooKeeper zk, Watcher watcher, double newValue) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putDouble(newValue);
        try {
            double oldValue = readWeightFromZookeeper(zk, watcher);
            System.out.println("Before Update: " + oldValue);
            System.out.println("NewValue: " + newValue);
            if(oldValue < newValue) {
                zk.setData(znode_weight, buf.array(), -1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return readWeightFromZookeeper(zk, watcher);
    }
}
