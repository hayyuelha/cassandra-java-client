package cassandra_java_client.client;

import java.util.Date;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	Querying twitter = new Querying("167.205.35.19", "hayyu");
    	twitter.register("icha", "ichajuga");
    	if (twitter.isUserFollows("hera", "icha")) {
    		System.out.println("icha follow hera");
    	} else {
    		System.out.println("icha belum follow hera");
    		twitter.follow("icha", "hera");
    		if (twitter.isUserFollows("hera", "icha")) {
        		System.out.println("icha follow hera");
        	} else {
        		System.out.println("icha belum follow hera");
        	}
    	}
    	twitter.tweet("icha", "lalallalaallalallalalala");
    	twitter.showTweets("icha", Querying.USER);
    	twitter.showTweets("icha", Querying.TIMELINE);
    	twitter.close();
    }
}
