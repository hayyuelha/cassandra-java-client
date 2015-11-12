package cassandra_java_client.client;

import java.util.Date;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;


public class Querying {
	
	private Cluster cluster;
	private Session session;
	private String keyspace;
	private QueryBuilder qb;
	private Statement statement;
	private ResultSet results;
	
	public static final String USER = "userline";
	public static final String TIMELINE = "timeline";
	
	public Querying (String host, String keyspace) {
		this.cluster = Cluster.builder().addContactPoint(host).build();
		this.session = cluster.connect(keyspace);
		this.keyspace = keyspace;
		this.qb = new QueryBuilder(cluster);
	}
	
	public void close () {
		this.cluster.close();
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
	
	public boolean login (String username, String password) {
		this.statement = this.qb.select().all().from(this.keyspace, "users")
				.where(QueryBuilder.eq("username", username));
		this.results = this.session.execute(this.statement);
		boolean exist = false;
		for (Row row: results) {
			exist = password.equals(row.getString("password"));
		}
		return exist;
	}
	
	public void follow (String username, String friend) {
		if (isUserExist(username) && isUserExist(friend)) {
			Date currentTime = new Date(); // UTC time; need to specify time zone
			this.statement = this.qb.insertInto(this.keyspace, "friends")
					.value("username", friend)
					.value("friend", username)
					.value("since", currentTime);
			this.session.execute(this.statement);
			this.statement = this.qb.insertInto(this.keyspace, "followers")
					.value("username", friend)
					.value("follower", username)
					.value("since", currentTime);
			this.session.execute(this.statement);
		} else {
			System.out.println("Cannot follow " + friend); //Is there any error message from cassandra for primary key violation?
		}
	}
	
	public void tweet (String username, String body) {
		if (isUserExist(username)) {
			UUID tweet_id = UUIDs.random();
			UUID time = UUIDs.timeBased();
			this.statement = this.qb.insertInto(this.keyspace, "tweets")
					.value("tweet_id", tweet_id)
					.value("username", username)
					.value("body", body);
			this.session.execute(this.statement);			
			this.statement = this.qb.insertInto(this.keyspace, "userline")
					.value("username", username)
					.value("time", time)
					.value("tweet_id", tweet_id);
			this.session.execute(this.statement);
			this.statement = this.qb.insertInto(this.keyspace, "timeline")
					.value("username", username)
					.value("time", time)
					.value("tweet_id", tweet_id);
			this.session.execute(this.statement);
			this.statement = this.qb.select("follower")
					.from(this.keyspace, "followers")
					.where(QueryBuilder.eq("username", username));
			this.results = this.session.execute(this.statement);
			for (Row row: this.results) {
				this.statement = this.qb.insertInto(this.keyspace, "timeline")
						.value("username", row.getString("follower"))
						.value("time", time)
						.value("tweet_id", tweet_id);
				this.session.execute(this.statement);				
			}
		}
	}
	
	public void showTweets (String username, String type) {
		if (isUserExist(username)) {
			this.statement = this.qb.select().from(this.keyspace, type)
					.where(QueryBuilder.eq("username", username));
			this.results = this.session.execute(this.statement);
			for (Row row: this.results) {
				this.statement = this.qb.select().from(this.keyspace, "tweets")
						.where(QueryBuilder.eq("tweet_id", row.getObject("tweet_id")));
				ResultSet tweet = this.session.execute(this.statement);
				for (Row r: tweet) {
		        	System.out.format("[%s] %s\n", 
		        			r.getString("username"),
		        			r.getString("body"));
				}
			}
		} else {
			System.out.println("Username " + username + " doesn't exist");
		}			
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
				.where(QueryBuilder.eq("username", user2))
				.and(QueryBuilder.eq("follower", user1));
		this.results = this.session.execute(this.statement);
		if (results.all().isEmpty())
			return false;
		return true;
	}	
}
