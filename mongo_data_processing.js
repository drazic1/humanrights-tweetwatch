// Data processing steps

// 1. Collect information about all users from data set

// all users
topusers = db.TweetWatch.aggregate(
	{ $match : { "user.followers_count" : { $gte : 3000 } } },
    { $group : {_id : "$user.id_str", total : { $sum : 1 } } },
    { $sort : {total : -1} }
        )

// place users in array
counter=0
topusers_array=[];
topusers.result.forEach(function(user){
	topusers_array[counter]=user._id;
	counter++
})

// creates a collection on all users and relevant info
db.users_collection.remove()
topusers_array.forEach(function(user){
	oneTweet = db.TweetWatch.findOne({ "user.id_str" : user }); 
	total = db.TweetWatch.find({ "user.id_str" : user }).count();
	userInfo = { "id" : user,  "name" : oneTweet.user.screen_name,
	     "followers" : oneTweet.user.followers_count, 
	     "friends" : oneTweet.user.friends_count, 
	     "tweets" : oneTweet.user.statuses_count, 
	     "total" : total };
     db.users_collection.insert(userInfo);
	 });

// Export user information (for user bar chart and tooltip)
mongo --quiet TwitterData --eval 'printjson( db.users_collection.find({},{"_id":0}).sort({"followers": -1}).toArray())' > userData.json


// 2. Create subcollection of data based on top users

// find top 1000 users and place in array
top1000users = db.users_collection.aggregate(
    { $sort : {followers : -1} },
    { $limit: 1000 }
        )

counter=0
top1000users_array=[];
top1000users.result.forEach(function(user){
	top1000users_array[counter]=user.id;
	counter++
})

// create subcollection of tweets that only belong to top 1000 users
skinny = db.TweetWatch.aggregate(
	{ $match : { "user.id_str" : { $in : top1000users_array } } },
	{ $sort : { "user.followers_count" : -1 } }
	)

db.TweetWatchSkinny1000.remove()
skinny.result.forEach(function(tweet){
     db.TweetWatchSkinny1000.insert(tweet);
	 });


// 3. Analyse user mentions, create user-user links
// find tweets with user mentions
results = db.TweetWatchSkinny1000.aggregate(
	{ $match : { "entities.user_mentions" : { $ne : [] } } },
	{ $unwind : "$entities.user_mentions" },
	{ $project : { "user_name" : "$user.screen_name", "user_id" : "$user.id_str", "mention" : "$entities.user_mentions.screen_name", "mention_id" : "$entities.user_mentions.id_str" } }
	)

// create links
db.userUserLinks.remove();
results.result.forEach(function(tweet){
	if ( (top1000users_array.indexOf(tweet.mention_id) != -1) && (tweet.user_name != tweet.mention) ) {
		if (db.userUserLinks.find({ "source" : tweet.user_name, "target": tweet.mention}).count() == 0) {
			link = { "source" : tweet.user_name, "source_type" : "user", "target" : tweet.mention, "target_type" : "user", "count" : 1 };
			db.userUserLinks.insert(link);
		} else {
			db.userUserLinks.update({ "source" : tweet.user_name, "target": tweet.mention }, { $inc : { "count" : 1 } } );
		}
	}
})

// export links for user to user graph
mongo --quiet TwitterData --eval 'printjson( db.userUserLinks.find({},{"_id":0}).sort({"count":-1}).toArray())' > userUserLinks.json

// 4. Analyze hashtags, create user-hashtag links

topHashtags = db.TweetWatchSkinny1000.aggregate(
	{ $match :  { "$entities.hashtags" : { $ne: [] } } },
 	{ $unwind : "$entities.hashtags"},
	{ $project : { "user_name" : "$user.screen_name", "user_id" : "$user.id_str", "hashtag" : "$entities.hashtags.text"} }
   )

db.userHashtagLinks.remove();
topHashtags.result.forEach(function(doc){
	if (db.userHashtagLinks.find({"source" : doc.user_name, "target" : doc.hashtag }).count() == 0) {
		link = { "source" : doc.user_name, "source_type" : "user", "target" : doc.hashtag, "target_type" : "hashtag", "count" : 1 };
		db.userHashtagLinks.insert(link);
	} else {
		db.userHashtagLinks.update( {"source" : doc.user_name, "target" : doc.hashtag }, { $inc : { "count" : 1 } } );
	}
})

// export links for user to hashtag graph
mongo --quiet TwitterData --eval 'printjson( db.userHashtagLinks.find({},{"_id":0}).sort({"count":-1}).toArray())' > userHashtagLinks.json

// export hashtag data for hashtag bar chart, do this in javascript?
// mongo --quiet TwitterData --eval 'printjson( db.userHashtagLinks.aggregate({$group:{_id:"$target",total:{$sum:"$count"}}},{ $sort : { total: -1} } ).toArray())' > hashtagData.json

// included  in the line above
db.userHashtagLinks.aggregate(
	    { $group : { _id : "$target", total : { $sum : "$count" } } },
	    { $sort : { total: -1} } )


// 5. Analyze domains, create user-domain links

// find all mentioned urls, sort by total count
topurls = db.TweetWatchSkinny1000.aggregate(
	{ $match : { "entities.urls" : { $ne : [] }}},
	{ $unwind : "$entities.urls"},
    { $group : {_id : "$entities.urls.expanded_url", total : { $sum : 1 } } },
    { $sort : { total : -1 }}   )

// create new collection with distinct urls
db.urlsMentionedByTop1000Users.remove()
topurls.result.forEach(function(mention){
	db.urlsMentionedByTop1000Users.insert({"url": mention._id, "total": mention.total});
	})

// run expandURLtop1000users.py //

// add domains to raw data
//first move all domain fields
db.TweetWatchSkinny1000.update(
	{ "entities.domain" : {$exists : true}}, 
	{ $unset : {"entities.domain" : ''}}, 
	{ multi: true}
	)

result = db.domainsMentionedByTop1000Users.find()
result.forEach(function(doc){
	db.TweetWatchSkinny1000.update(
		{ "entities.urls.expanded_url" : doc.shortUrl },
		{ $push: { "entities.domain" : doc.domain } },
		{ multi: true}
		)
})

// find docs where new field exists
db.TweetWatchSkinny1000.find({'entities.domain': { $exists : true}})

// find user domain links
tweets = db.TweetWatchSkinny1000.aggregate([
		{ $project: { "id": "$user.id_str", "name": "$user.screen_name", "domain": "$entities.domain", _id:0 }},
		{ $match: { "domain" : { $exists: true }}}, 
		{ $unwind: "$domain"}
			])

// create new collection
db.userDomainLinks.remove()
tweets.result.forEach(function(tweet) { 
	if(db.userDomainLinks.find({ "source": tweet.name, "target": tweet.domain}).count()==0){
		link = { "source": tweet.name, "source_type" : "user", "target": tweet.domain, "target_type" : "domain", "count":1};
		db.userDomainLinks.insert( link );
	}else{
		db.userDomainLinks.update( { "source": tweet.name, "target": tweet.domain}, { $inc : { "count" : 1 } } )
	}
})
// export links for user to domain graph
mongo --quiet TwitterData --eval 'printjson( db.userDomainLinks.find({},{"_id":0}).sort({"count":-1}).toArray())' > userDomainLinks.json

// export domain data for domain bar chart, do in javascript
// $ mongo --quiet TwitterData --eval 'printjson( db.userDomainLinks.aggregate({$group:{_id:"$target",total:{$sum:"$count"}}},{ $sort : { total: -1} } ).toArray())' > hashtagData.json

// find top domains for domain bar chart
db.userDomainLinks.aggregate(
	    { $group : { _id : "$target", total : { $sum : "$count" } } },
	    { $sort : { total: -1} } )

// 7. Create domain-hashtag links
 
tweets = db.TweetWatchSkinny1000.aggregate(
		{ $match: { "entities.domain" : { $exists: true }, "entities.hashtags" : {$ne : []}}},
		{ $project: { "hashtag": "$entities.hashtags.text", "domain": "$entities.domain", _id:0 }},
		{ $unwind : "$hashtag"},
		{ $unwind : "$domain"}
			)

db.hashtagDomainLinks.remove();
tweets.result.forEach(function(tweet) {
	if(db.hashtagDomainLinks.find({ "source": tweet.hashtag, "target": tweet.domain}).count()==0){
		link = { "source": tweet.hashtag, "source_type" : "hashtag", "target": tweet.domain, "target_type" : "domain", "count":1};
		db.hashtagDomainLinks.insert( link );
	}else{
		db.hashtagDomainLinks.update( { "source": tweet.hashtag, "target": tweet.domain}, { $inc : { "count" : 1 } } )
	}
})

// export links for user to domain graph
mongo --quiet TwitterData --eval 'printjson( db.hashtagDomainLinks.find({},{"_id":0}).sort({"count":-1}).toArray())' > hashtagDomainLinks.json
