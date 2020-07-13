db.orders.drop()

db.orders.insertMany([
	{"name": "Alex", "fruit_id": 1, "happy": false, "review": "Was in transit for five days, arrived rotten."},
	{"name": "James", "fruit_id": 2, "happy": true},
	{"name": "Alice", "fruit_id": 3, "happy": true, "review": "Will use in salad, great fruit!"}]);
