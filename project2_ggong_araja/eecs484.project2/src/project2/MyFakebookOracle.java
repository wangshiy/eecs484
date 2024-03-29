package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.TreeSet;
import java.util.Vector;

//Test
public class MyFakebookOracle extends FakebookOracle {
	
	static String prefix = "unnamed.";
	
	// You must use the following variable as the JDBC connection
	Connection oracleConnection = null;
	
	// You must refer to the following variables for the corresponding tables in your database
	String cityTableName = null;
	String userTableName = null;
	String friendsTableName = null;
	String currentCityTableName = null;
	String hometownCityTableName = null;
	String programTableName = null;
	String educationTableName = null;
	String eventTableName = null;
	String participantTableName = null;
	String albumTableName = null;
	String photoTableName = null;
	String coverPhotoTableName = null;
	String tagTableName = null;
	
	// DO NOT modify this constructor
	public MyFakebookOracle(String u, Connection c) {
		super();
		String dataType = u;
		oracleConnection = c;
		// You will use the following tables in your Java code
		cityTableName = prefix+dataType+"_CITIES";
		userTableName = prefix+dataType+"_USERS";
		friendsTableName = prefix+dataType+"_FRIENDS";
		currentCityTableName = prefix+dataType+"_USER_CURRENT_CITY";
		hometownCityTableName = prefix+dataType+"_USER_HOMETOWN_CITY";
		programTableName = prefix+dataType+"_PROGRAMS";
		educationTableName = prefix+dataType+"_EDUCATION";
		eventTableName = prefix+dataType+"_USER_EVENTS";
		albumTableName = prefix+dataType+"_ALBUMS";
		photoTableName = prefix+dataType+"_PHOTOS";
		tagTableName = prefix+dataType+"_TAGS";
	}
	
	
	@Override
	// ***** Query 0 *****
	// This query is given to your for free;
	// You can use it as an example to help you write your own code
	//
	public void findMonthOfBirthInfo() throws SQLException{ 
		
		// Scrollable result set allows us to read forward (using next())
		// and also backward.  
		// This is needed here to support the user of isFirst() and isLast() methods,
		// but in many cases you will not need it.
		// To create a "normal" (unscrollable) statement, you would simply call
		// Statement stmt = oracleConnection.createStatement();
		//
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
		        ResultSet.CONCUR_READ_ONLY);
		
		// For each month, find the number of friends born that month
		// Sort them in descending order of count
		ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from "+
				userTableName+
				" where month_of_birth is not null group by month_of_birth order by 1 desc");
		
		this.monthOfMostFriend = 0;
		this.monthOfLeastFriend = 0;
		this.totalFriendsWithMonthOfBirth = 0;
		
		// Get the month with most friends, and the month with least friends.
		// (Notice that this only considers months for which the number of friends is > 0)
		// Also, count how many total friends have listed month of birth (i.e., month_of_birth not null)
		//
		while(rst.next()) {
			int count = rst.getInt(1);
			int month = rst.getInt(2);
			if (rst.isFirst())
				this.monthOfMostFriend = month;
			if (rst.isLast())
				this.monthOfLeastFriend = month;
			this.totalFriendsWithMonthOfBirth += count;
		}
		
		// Get the names of friends born in the "most" month
		rst = stmt.executeQuery("select user_id, first_name, last_name from "+
				userTableName+" where month_of_birth="+this.monthOfMostFriend);
		while(rst.next()) {
			Long uid = rst.getLong(1);
			String firstName = rst.getString(2);
			String lastName = rst.getString(3);
			this.friendsInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Get the names of friends born in the "least" month
		rst = stmt.executeQuery("select first_name, last_name, user_id from "+
				userTableName+" where month_of_birth="+this.monthOfLeastFriend);
		while(rst.next()){
			String firstName = rst.getString(1);
			String lastName = rst.getString(2);
			Long uid = rst.getLong(3);
			this.friendsInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
		}
		
		// Close statement and result set
		rst.close();
		stmt.close();
	}

	
	
	@Override
	// ***** Query 1 *****
	// Find information about friend names:
	// (1) The longest last name (if there is a tie, include all in result)
	// (2) The shortest last name (if there is a tie, include all in result)
	// (3) The most common last name, and the number of times it appears (if there is a tie, include all in result)
	//
	public void findNameInfo() throws SQLException {
	/*
		Find the following information from your database and store the information as shown 
		this.longestLastNames.add("JohnJacobJingleheimerSchmidt");
		this.shortestLastNames.add("Ng");
		this.shortestLastNames.add("Fu");
		this.shortestLastNames.add("Wu");
		this.mostCommonLastNames.add("Wang");
		this.mostCommonLastNames.add("Smith");
		this.mostCommonLastNamesCount = 10;
	*/
		
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rst = stmt.executeQuery("select last_name "
		+ "from " + userTableName
		+ " where last_name is not null "
		+ "group by last_name order by length(last_name) desc");

		// Iterate to the end of the table where the shortest last_name is
		String baseString = null;
		while(rst.next()) {
			String LastName = rst.getString(1);
			if (rst.isFirst()) {
				baseString = LastName;
			}
			if(LastName.length() == baseString.length() ) {
				this.longestLastNames.add(LastName);
			}
		}
		
		// Iterate back from the end until a last_name longer than the shortest last_name is encountered
		while(rst.previous()) {
			String LastName = rst.getString(1);
			if (rst.isLast()) {
				baseString = LastName;
			}
			if(LastName.length() == baseString.length() ) {
				this.shortestLastNames.add(LastName);
			}
			else break;
		}

		rst = stmt.executeQuery("SELECT last_name, count(last_name) "
		+ "FROM " + userTableName + " "
		+ "WHERE last_name IS NOT NULL "
		+ "GROUP BY last_name ORDER BY 2 DESC");

    // Inset most common last name
    int baseCount = 0;
		while(rst.next()) {
      String lastName = rst.getString(1);
			int count = rst.getInt(2);
			if (rst.isFirst()) {
				baseCount = count;
        this.mostCommonLastNamesCount = baseCount;
			}
      if(count != baseCount) break;
			this.mostCommonLastNames.add(lastName);
		}
	
		rst.close();	
		stmt.close();
		
	}
	
	@Override
	// ***** Query 2 *****
	// Find the user(s) who have no friends in the network
	//
	// Be careful on this query!
	// Remember that if two users are friends, the friends table
	// only contains the pair of user ids once, subject to 
	// the constraint that user1_id < user2_id
	//
	public void lonelyFriends() throws SQLException {
	/*
		Find the following information from your database and store the information as shown 
		this.lonelyFriends.add(new UserInfo(10L, "Billy", "SmellsFunny"));
		this.lonelyFriends.add(new UserInfo(11L, "Jenny", "BadBreath"));
		this.countLonelyFriends = 2;
	*/
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rst = stmt.executeQuery("select U.user_id, U.first_name, U.last_name from "+ userTableName +" U where U.user_id not in (select distinct F.user1_id from " + friendsTableName + " F union select distinct F.user2_id from "+ friendsTableName + " F)");

		this.countLonelyFriends = 0;
		while(rst.next()){	//read individual users
			int user_id = rst.getInt(1);
			String FirstName = rst.getString(2);
			String LastName = rst.getString(3);
			this.lonelyFriends.add(new UserInfo(new Long(user_id), FirstName, LastName));
			this.countLonelyFriends++;
		}

		// Close statement and result set
		rst.close();
		stmt.close();
	}
	 

	@Override
	// ***** Query 3 *****
	// Find the users who still live in their hometowns
	// (I.e., current_city = hometown_city)
	//	
	public void liveAtHome() throws SQLException {
	/*
		this.liveAtHome.add(new UserInfo(11L, "Heather", "Hometowngirl"));
		this.countLiveAtHome = 1;
	*/
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

		ResultSet rst = stmt.executeQuery("select U.user_id, U.first_name, U.last_name from " + userTableName +" U, " + 
				currentCityTableName + " C, " + hometownCityTableName +  
				" H where U.user_id = C.user_id AND U.user_id = H.user_id AND C.current_city_id = H.hometown_city_id");

		this.countLiveAtHome = 0;

		while(rst.next()){
			int user_id = rst.getInt(1);
			String FirstName = rst.getString(2);
			String LastName = rst.getString(3);
			this.liveAtHome.add(new UserInfo(new Long(user_id), FirstName, LastName));
			this.countLiveAtHome = this.countLiveAtHome + 1;
		}
		
		// Close statement and result set
		rst.close();
		stmt.close();
	}

	@Override
	// **** Query 4 ****
	// Find the top-n photos based on the number of tagged users
	// If there are ties, choose the photo with the smaller numeric PhotoID first
	// 
	public void findPhotosWithMostTags(int n) throws SQLException { 
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

    ResultSet rst = stmt.executeQuery("SELECT base.tag_photo_id, T.tag_subject_id, P.photo_link, A.album_id, A.album_name, base.count, U.first_name, U.last_name "
                + "FROM "
	                  + "(SELECT tag_photo_id, count(tag_photo_id) as count FROM " + tagTableName
	                  + " GROUP BY tag_photo_id "
                    + "ORDER BY 2 DESC, 1 ASC) base "
                + "FULL OUTER JOIN " + tagTableName + " T ON T.tag_photo_id=base.tag_photo_id "
                + "FULL OUTER JOIN " + photoTableName + " P ON base.tag_photo_id=P.photo_id "
                + "FULL OUTER JOIN " + albumTableName + " A ON P.album_id=A.album_id "
                + "FULL OUTER JOIN " + userTableName + " U ON T.tag_subject_id=U.user_id");
		
		String base = null;
    rst.next();

    // Get top n photos
		for(int i = 0; i < n; i++){
			String photoId = rst.getString(1);
			String userId = rst.getString(2);
			String photoCaption = null;
			String photoLink = rst.getString(3);
			String albumId = rst.getString(4);
			String albumName = rst.getString(5);
			PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);

			// Get tagged users
			TaggedPhotoInfo tp = new TaggedPhotoInfo(p);
			base = photoId;
			
		  do {
        if(!photoId.equals(base) ) {
          break;
        }

				photoId = rst.getString(1);
				long user_id = rst.getLong(2);
				String first_name = rst.getString(7);
				String last_name = rst.getString(8);
				tp.addTaggedUser(new UserInfo(user_id, first_name, last_name));
				this.photosWithMostTags.add(tp);
			} while(rst.next());
		}
		
		// Close statement and result set
		rst.close();
		stmt.close();
	}

	@Override
	// **** Query 5 ****
	// Find suggested "match pairs" of friends, using the following criteria:
	// (1) One of the friends is female, and the other is male
	// (2) Their age difference is within "yearDiff"
	// (3) They are not friends with one another
	// (4) They should be tagged together in at least one photo
	//
	// You should up to n "match pairs"
	// If there are more than n match pairs, you should break ties as follows:
	// (i) First choose the pairs with the largest number of shared photos
	// (ii) If there are still ties, choose the pair with the smaller user_id for the female
	// (iii) If there are still ties, choose the pair with the smaller user_id for the male
	//
	public void matchMaker(int n, int yearDiff) throws SQLException { 
	/*  Long girlUserId = 123L;
		String girlFirstName = "girlFirstName";
		String girlLastName = "girlLastName";
		int girlYear = 1988;
		Long boyUserId = 456L;
		String boyFirstName = "boyFirstName";
		String boyLastName = "boyLastName";
		int boyYear = 1986;
		MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
				girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
		String sharedPhotoId = "12345678";
		String sharedPhotoAlbumId = "123456789";
		String sharedPhotoAlbumName = "albumName";
		String sharedPhotoCaption = "caption";
		String sharedPhotoLink = "link";
		mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
				sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
		this.bestMatches.add(mp);
	*/
		
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

		//SHOULD IT BE <= OR < FOR YEARDIFF?????

    ResultSet rst = stmt.executeQuery("SELECT tagged.tagUser1, U1.first_name, U1.last_name, U1.year_of_birth, tagged.tagUser2, U2.first_name, U2.last_name, U2.year_of_birth, tagged.photoId, A.album_id, A.album_name, P.photo_caption, P.photo_link, tagged.count "
        + "FROM (SELECT T1.tag_subject_id as tagUser1, T2.tag_subject_id as tagUser2, T1.tag_photo_id as photoId, count(*) as count "
            + "FROM " + tagTableName + " T1, " + tagTableName + " T2 "
            + "WHERE T1.tag_photo_id = T2.tag_photo_id AND T1.tag_subject_id != T2.tag_subject_id AND 3>0 "
            + "GROUP BY T1.tag_subject_id, T2.tag_subject_id, T1.tag_photo_id ORDER BY count DESC) tagged "
        + "FULL OUTER JOIN " + userTableName + " U1 ON tagged.tagUser1=U1.user_id "
        + "FULL OUTER JOIN " + userTableName + " U2 ON tagged.tagUser2=U2.user_id "
        + "FULL OUTER JOIN " + photoTableName + " P ON tagged.photoId=P.photo_id "
        + "FULL OUTER JOIN " + albumTableName + " A on P.album_id=A.album_id "
        + "WHERE tagged.tagUser1 IS NOT NULL AND tagged.tagUser2 IS NOT NULL AND U1.gender='female' AND U2.gender='male' AND ABS(U1.year_of_birth-U2.year_of_birth)<=" + yearDiff + " "
        + "AND tagged.tagUser1 NOT IN ( "
            + "SELECT F.user1_id FROM " + friendsTableName + " F "
            + "WHERE (tagged.tagUser1=F.user1_id AND tagged.tagUser2=F.user2_id) OR (tagged.tagUser1=F.user2_id AND tagged.tagUser2=F.user1_id)) "
        + "ORDER BY tagged.count DESC, tagged.tagUser1 ASC, tagged.tagUser2 ASC");

    for(int i = 0; i < n; i++) {
      if(!rst.next()) break;

      Long girlUserId = rst.getLong(1);
      String girlFirstName = rst.getString(2);
      String girlLastName = rst.getString(3);
      int girlYear = rst.getInt(4);
      Long boyUserId = rst.getLong(5);
      String boyFirstName = rst.getString(6);
      String boyLastName = rst.getString(7);
      int boyYear = rst.getInt(8);
      MatchPair mp = new MatchPair(girlUserId, girlFirstName, girlLastName, 
		      girlYear, boyUserId, boyFirstName, boyLastName, boyYear);
      String sharedPhotoId = rst.getString(9);
      String sharedPhotoAlbumId = rst.getString(10);
      String sharedPhotoAlbumName = rst.getString(11);
      String sharedPhotoCaption = rst.getString(12);
      String sharedPhotoLink = rst.getString(13);
      mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId, 
		      sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
      this.bestMatches.add(mp);
    }

		// Close statement and result set
		rst.close();
		stmt.close();
	}
	
	// **** Query 6 ****
	// Suggest friends based on mutual friends
	// 
	// Find the top n pairs of users in the database who share the most
	// friends, but such that the two users are not friends themselves.
	//
	// Your output will consist of a set of pairs (user1_id, user2_id)
	// No pair should appear in the result twice; you should always order the pairs so that
	// user1_id < user2_id
	//
	// If there are ties, you should give priority to the pair with the smaller user1_id.
	// If there are still ties, give priority to the pair with the smaller user2_id.
	//
	@Override
	public void suggestFriendsByMutualFriends(int n) throws SQLException {
	/*
		Long user1_id = 123L;
		String user1FirstName = "Friend1FirstName";
		String user1LastName = "Friend1LastName";
		Long user2_id = 456L;
		String user2FirstName = "Friend2FirstName";
		String user2LastName = "Friend2LastName";
		FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

		p.addSharedFriend(567L, "sharedFriend1FirstName", "sharedFriend1LastName");
		p.addSharedFriend(678L, "sharedFriend2FirstName", "sharedFriend2LastName");
		p.addSharedFriend(789L, "sharedFriend3FirstName", "sharedFriend3LastName");
		this.suggestedFriendsPairs.add(p);
	*/

		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);		

    ResultSet rst = stmt.executeQuery("SELECT mutualFriends.user1, U1.first_name, U1.last_name, mutualFriends.user2, U2.first_name, U2.last_name, mutualList.user3, U3.first_name, U3.last_name, mutualFriends.count "
      + "FROM ( "
          + "SELECT mutual.person1 as user1, mutual.person2 as user2, count(*) as count FROM "
              + "(SELECT F1.user1_id as person1, F2.user1_id as person2, F2.user2_id as person3 "
              + "FROM " + friendsTableName + " F1, " + friendsTableName + " F2 "
              + "WHERE F1.user2_id=F2.user2_id AND F1.user1_id!=F2.user1_id "
              + "UNION ALL "
              + "SELECT F1.user1_id as person1, F2.user2_id as person2, F2.user1_id as person3 "
              + "FROM " + friendsTableName + " F1, " + friendsTableName + " F2 "
              + "WHERE F1.user2_id=F2.user1_id AND F1.user1_id!=F2.user2_id "
              + "UNION ALL "
              + "SELECT F1.user2_id as person1, F2.user2_id as person2, F2.user1_id as person3 "
              + "FROM " + friendsTableName + " F1, " + friendsTableName + " F2 "
              + "WHERE F1.user1_id=F2.user1_id AND F1.user2_id!=F2.user2_id "
              + "ORDER BY 3 ASC) mutual "
          + "GROUP BY mutual.person1, mutual.person2 ORDER BY 3 DESC) mutualFriends "
      + "FULL OUTER JOIN ( "
          + "SELECT F1.user1_id as user1, F2.user1_id as user2, F2.user2_id as user3 "
              + "FROM " + friendsTableName + " F1, " + friendsTableName + " F2 "
              + "WHERE F1.user2_id=F2.user2_id AND F1.user1_id!=F2.user1_id "
              + "UNION ALL "
              + "SELECT F1.user1_id, F2.user2_id, F2.user1_id "
              + "FROM " + friendsTableName + " F1, " + friendsTableName + " F2 "
              + "WHERE F1.user2_id=F2.user1_id AND F1.user1_id!=F2.user2_id "
              + "UNION ALL "
              + "SELECT F1.user2_id, F2.user2_id, F2.user1_id "
              + "FROM " + friendsTableName + " F1, " + friendsTableName + " F2 "
              + "WHERE F1.user1_id=F2.user1_id AND F1.user2_id!=F2.user2_id "
              + "ORDER BY 3 ASC) mutualList "
      + "ON mutualFriends.user1=mutualList.user1 AND mutualFriends.user2=mutualList.user2 "
      + "FULL OUTER JOIN " + userTableName + " U1 ON U1.user_id=mutualFriends.user1 "
      + "FULL OUTER JOIN " + userTableName + " U2 ON U2.user_id=mutualFriends.user2 "
      + "FULL OUTER JOIN " + userTableName + " U3 ON U3.user_id=mutualList.user3 "
      + "WHERE mutualFriends.user1<mutualFriends.user2 AND mutualFriends.user1 NOT IN ( "
            + "SELECT F.user1_id FROM " + friendsTableName + " F "
            + "WHERE (mutualFriends.user1=F.user1_id AND mutualFriends.user2=F.user2_id)) "
      + "ORDER BY mutualFriends.count DESC, mutualFriends.user1 ASC, mutualFriends.user2 ASC, mutualList.user3 ASC"
    );

    Long base1_id = null;
    Long base2_id = null;
    rst.next();
    for(int i = 0; i < n; i++) {
      // Get friend pair
		  Long user1_id = rst.getLong(1);
	    String user1FirstName = rst.getString(2);
	    String user1LastName = rst.getString(3);
	    Long user2_id = rst.getLong(4);
	    String user2FirstName = rst.getString(5);
	    String user2LastName = rst.getString(6);
	    FriendsPair p = new FriendsPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
      
      base1_id = user1_id;
      base2_id = user2_id;

      // Get shared friends
      do {
        user1_id = rst.getLong(1);
        user2_id = rst.getLong(4);
        if(!user1_id.equals(base1_id) || !user2_id.equals(base2_id)) break;

        Long sharedFriendId = rst.getLong(7);
        String sharedFriendFirstName = rst.getString(8);
        String sharedFriendLastName = rst.getString(9);
		    p.addSharedFriend(sharedFriendId, sharedFriendFirstName, sharedFriendLastName);
		    this.suggestedFriendsPairs.add(p);
      } while(rst.next());
    }

		// Close statement and result set
		rst.close();
		stmt.close();
	}
	
	
	@Override
	// ***** Query 7 *****
	// Given the ID of a user, find information about that
	// user's oldest friend and youngest friend
	// 
	// If two users have exactly the same age, meaning that they were born
	// on the same day, then assume that the one with the larger user_id is older
	//
	public void findAgeInfo(Long user_id) throws SQLException {
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

    UserInfo youngest = null;
    UserInfo oldest = null;

    // Get oldest friend
		ResultSet rst = stmt.executeQuery("SELECT friends.person1, friends.person2, U.first_name, U.last_name, U.day_of_birth, U.month_of_birth, U.year_of_birth "
    + "FROM ("
      + "SELECT F.user1_id as person1, F.user2_id as person2 "
      + "FROM " + friendsTableName + " F "
      + "WHERE F.user1_id=" + user_id + " "
      + "UNION ALL "
      + "SELECT F.user2_id as person1, F.user1_id as person2 "
      + "FROM " + friendsTableName + " F "
      + "WHERE F.user2_id=" + user_id + ") friends "
    + "INNER JOIN " + userTableName + " U ON friends.person2=U.user_id "
    + "ORDER BY U.year_of_birth ASC, U.month_of_birth ASC, U.day_of_birth ASC, friends.person2 DESC"
		);

    if(rst.next() ) {
      oldest = new UserInfo(rst.getLong(2), rst.getString(3), rst.getString(4));
      this.oldestFriend = oldest;
    }

    // Get youngest friend
		rst = stmt.executeQuery("SELECT friends.person1, friends.person2, U.first_name, U.last_name, U.day_of_birth, U.month_of_birth, U.year_of_birth "
    + "FROM ("
      + "SELECT F.user1_id as person1, F.user2_id as person2 "
      + "FROM " + friendsTableName + " F "
      + "WHERE F.user1_id=" + user_id + " "
      + "UNION ALL "
      + "SELECT F.user2_id as person1, F.user1_id as person2 "
      + "FROM " + friendsTableName + " F "
      + "WHERE F.user2_id=" + user_id + ") friends "
    + "INNER JOIN " + userTableName + " U ON friends.person2=U.user_id "
    + "ORDER BY U.year_of_birth DESC, U.month_of_birth DESC, U.day_of_birth DESC, friends.person2 ASC"
		);

    if(rst.next() ) {
      youngest = new UserInfo(rst.getLong(2), rst.getString(3), rst.getString(4));
      this.youngestFriend = youngest;
    }
		
		// Close statement and result set
		rst.close();
		stmt.close();
	}
	
	
	@Override
	// ***** Query 8 *****
	// 
	// Find the name of the city with the most events, as well as the number of 
	// events in that city.  If there is a tie, return the names of all of the (tied) cities.
	//
	public void findEventCities() throws SQLException {
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		
		ResultSet rst = stmt.executeQuery("SELECT COUNT(E.event_city_id), C.city_name"
		+ " FROM " + eventTableName + " E, " + cityTableName + " C" 
		+ " WHERE E.event_city_id = C.city_id" 
		+ " GROUP BY C.city_name" 
		+ " HAVING COUNT(E.event_id) = (SELECT MAX(COUNT(*)) FROM " + eventTableName + " GROUP BY event_city_id)"
		);
		
		while(rst.next()) {
			this.eventCount = rst.getInt(1);
			this.popularCityNames.add(rst.getString(2));
		}
		
		// Close statement and result set
		rst.close();
		stmt.close();
	}
	
	@Override
//	 ***** Query 9 *****
	//
	// Find pairs of potential siblings and print them out in the following format:
	//   # pairs of siblings
	//   sibling1 lastname(id) and sibling2 lastname(id)
	//   siblingA lastname(id) and siblingB lastname(id)  etc.
	//
	// A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
	// if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
	// on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
	//  
	//
	public void findPotentialSiblings() throws SQLException {
	/*
		Long user1_id = 123L;
		String user1FirstName = "Friend1FirstName";
		String user1LastName = "Friend1LastName";
		Long user2_id = 456L;
		String user2FirstName = "Friend2FirstName";
		String user2LastName = "Friend2LastName";
		SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
		this.siblings.add(s);
	*/
		Statement stmt = oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

    ResultSet rst = stmt.executeQuery("SELECT U1.user_id, U1.first_name, U1.last_name, U2.user_id, U2.first_name, U2.last_name" 
    + " FROM " + friendsTableName + " F, " + userTableName + " U1, " + userTableName + " U2," + hometownCityTableName + " H1, " + hometownCityTableName + " H2" 
    + " WHERE F.user1_id = U1.user_id AND F.user2_id = U2.user_id AND " + " H1.user_id = U1.user_id AND H2.user_id = U2.user_id AND "
    + " U1.last_name = U2.last_name AND " + " H1.hometown_city_id = H2.hometown_city_id AND " + " ABS(U1.year_of_birth - U2.year_of_birth) < 10"
    + " ORDER BY U1.user_id ASC, U2.user_id ASC"
    );

    while(rst.next()) {
      Long user1_id = rst.getLong(1);
      String user1FirstName = rst.getString(2);
      String user1LastName = rst.getString(3);
      Long user2_id = rst.getLong(4);
      String user2FirstName = rst.getString(5);
      String user2LastName = rst.getString(6);
      SiblingInfo s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
      this.siblings.add(s);
    }

		rst.close();
		stmt.close();
	}
}
