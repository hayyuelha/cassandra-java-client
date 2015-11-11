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
        Cluster cluster;
        Session session;
        
        cluster = Cluster.builder().addContactPoint("167.205.35.19").build();
        session = cluster.connect("hayyu");
        
//        ResultSet results = session.execute("SELECT * FROM users");
//        for (Row row : results) {
//        	System.out.format("%s %s\n", row.getString("username"), row.getString("password"));
//        }
        
        Date currTime = new Date();
        QueryBuilder qb = new QueryBuilder(cluster);
        Statement s = qb.insertInto("hayyu", "friends")
				.value("username", "hera")
				.value("friend", "minerva")
				.value("since", currTime);
        session.execute(s);
        
//        s = qb.select().all().from("hayyu", "friend");
//        ResultSet result = session.execute(s);
        cluster.close();
        
    }
}
