/**
 * 
 */
package cee.bigdata.hadoop.zookeeper;

import java.io.IOException;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
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
	
	public void exists(String path) throws KeeperException, InterruptedException, JsonProcessingException {
		zk.exists(path, this);
	}
	
	public void getState() throws KeeperException, InterruptedException, JsonProcessingException {
		System.out.println(zk.getState());
	}
	
	
	
	public static void main(String[] args) throws Exception {
		System.out.println("================");
		
		try {
			ZooTester zk = new ZooTester("115.28.109.129");
//			zk.getState();
//			zk.exists();
			
//			int random = RandomUtils.nextInt(100);
//			System.out.println(random);
			
//			zk.createZnode("/javatest/lock1", "Cee-Test".getBytes());
			
			zk.exists("/javatest/lock1");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		Thread.sleep(Long.MAX_VALUE);

	}


	@Override
	public void process(WatchedEvent arg0) {
		try {
			System.out.println("WatchedEvent: " + mapper.writeValueAsString(arg0));
			
			if (arg0.getType() == Event.EventType.NodeDeleted) {
				System.out.println("Scott close the program !!!");
			}
			if (arg0.getType() == Event.EventType.NodeCreated) {
				System.out.println("Scott start the program ---");
			}
			
			exists("/javatest/lock1");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
