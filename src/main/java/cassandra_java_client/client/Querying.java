package cassandra_java_client.client;

import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;


public class Querying {
	
	private Cluster cluster;
	private Session session;
	private String keyspace;
	private QueryBuilder qb;
	private Statement statement;
	private ResultSet results;
	
	public Querying (String host, String keyspace) {
		this.cluster = Cluster.builder().addContactPoint(host).build();
		this.session = cluster.connect(keyspace);
		this.keyspace = keyspace;
		this.qb = new QueryBuilder(cluster);
	}
	
	public void register (String username, String password) {
		if (!isUserExist(username)) {
			this.statement = this.qb.insertInto(this.keyspace, "users").value("username", username)
					.value("password", password);
			this.session.execute(this.statement);
		} else {
			System.out.println("User already exists."); //Is there any error message from cassandra for primary key violation?
		}
		
	}
	
	public void follow (String username, String friend) {
		if (isUserExist(username) && isUserExist(friend)) {
			Date currentTime = new Date(); // UTC time; need to specify time zone
			this.statement = this.qb.insertInto(this.keyspace, "friends")
					.value("username", username)
					.value("friend", friend)
					.value("since", currentTime);
			this.session.execute(this.statement);
			this.statement = this.qb.insertInto(this.keyspace, "followers")
					.value("username", username)
					.value("follow", friend)
					.value("since", currentTime);
			this.session.execute(this.statement);
		} else {
			System.out.println("Cannot follow " + friend); //Is there any error message from cassandra for primary key violation?
		}
		
	}
	
	public void tweet (String username, String body) {
		
	}
	
	
	
	// Support methods
	
	public boolean isUserExist (String username) {
		this.statement = this.qb.select().all().from(this.keyspace, "users")
				.where(QueryBuilder.eq("username", username));
		this.results = this.session.execute(this.statement);
		if (results.all().isEmpty())
			return false;
		return true;		
	}
	
	// What's the difference between friend and follow?
	public boolean areUsersFriends (String user1, String user2) {
		if (!isUserExist(user1) || !isUserExist(user2))
			return false;
		this.statement = this.qb.select().all().from(this.keyspace, "friends")
				.where(QueryBuilder.eq("username", user1))
				.and(QueryBuilder.eq("friend", user2));
		this.results = this.session.execute(this.statement);
		if (results.all().isEmpty())
			return false;
		return true;
	}
	
	public boolean isUserFollows (String user1, String user2) {
		if (!isUserExist(user1) || !isUserExist(user2))
			return false;
		this.statement = this.qb.select().all().from(this.keyspace, "followers")
				.where(QueryBuilder.eq("username", user1))
				.and(QueryBuilder.eq("follower", user2));
		this.results = this.session.execute(this.statement);
		if (results.all().isEmpty())
			return false;
		return true;
	}
	
	
	
}
