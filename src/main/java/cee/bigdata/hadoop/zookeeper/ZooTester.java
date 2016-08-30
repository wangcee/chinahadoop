/**
 * 
 */
package cee.bigdata.hadoop.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * cee.bigdata.hadoop.zookeeper.ZooTester.java
 *
 * @author WangCeeAsus
 *
 * @version $Revision:$
 *          $Author:$
 */
public class ZooTester implements Watcher {
	
	ObjectMapper mapper = new ObjectMapper();

	ZooKeeper zk;
	
	
	public ZooTester(String hostPort) throws KeeperException, IOException {
		zk = new ZooKeeper(hostPort, 3000, this);
	}
	
	public void createZnode(String path, byte[] data) throws KeeperException, InterruptedException {
		zk.create(path, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
	}
	
	public void exists() throws KeeperException, InterruptedException, JsonProcessingException {
		System.out.println(mapper.writeValueAsString(zk.exists("/javatest", false)));
	}
	
	public void getState() throws KeeperException, InterruptedException, JsonProcessingException {
		System.out.println(zk.getState());
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println("================");
		
		ZooTester zk = new ZooTester("115.28.109.129:2181");
		zk.getState();
		zk.exists();
//		zk.createZnode("/javatest/cee", "aaa".getBytes());
	}


	@Override
	public void process(WatchedEvent arg0) {
		try {
			System.out.println(mapper.writeValueAsString(arg0));
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
}
