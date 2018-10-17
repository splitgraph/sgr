db.stuff.drop()
db.enforcement_visits.drop()

db.stuff.insertMany([
	{"name": "Alex", "duration": 12, "happy": false},
	{"name": "James", "duration": 2, "happy": true},
	{"name": "Alice", "duration": 98765, "happy": true}]);
	

db.enforcement_visits.insertMany([
    {"client_id": "JEEX1ABC1", "visit_ts": new Date("2017-08-30T11:00:00Z"), "visit_reason": "TV license expired",
        "visit_result": "Convinced to buy TV license."},
    {"client_id": "ASSP1GHI1", "visit_ts": new Date("2017-08-13T12:23:34"), "visit_reason": "Not responded to letter",
        "visit_result": "Asked to come back with a warrant."},
    {"client_id": "MTTS5DEF1", "visit_ts": new Date("2018-08-15T12:13:14"), "visit_reason": "Declaration expired",
        "visit_result": "Wasn't home."}
])
