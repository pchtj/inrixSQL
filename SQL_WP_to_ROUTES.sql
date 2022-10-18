######################################################### TABLE trips


SELECT * FROM public.final_waypoints_; 
 
CREATE TABLE result_ (
	trip_id text,
	route geometry,
	weight_class int,
	trip_type int);
	
INSERT INTO result_ (trip_id, route, weight_class, trip_type) (SELECT a.trip_id, ST_MakeLine(a.geom ORDER BY a.waypoint_seq) as route , 
										 AVG(a.wt_cls) as weight_class, AVG(a.trip_type) as trip_type FROM
(SELECT * FROM public.final_waypoints_) as a
GROUP BY a.trip_id);

SELECT * FROM public.result_;

######################################################### TABLE trips_04, trips_07, and trips_10

SELECT * FROM public.final_waypoints_04; 
 
CREATE TABLE result_04 (
	trip_id text,
	route geometry,
	weight_class int,
	trip_type int);
	
INSERT INTO result_04 (trip_id, route, weight_class, trip_type) (SELECT a.trip_id, ST_MakeLine(a.geom ORDER BY a.waypoint_seq) as route , 
										 AVG(a.wt_cls) as weight_class, AVG(a.trip_type) as trip_type FROM
(SELECT * FROM public.final_waypoints_04) as a
GROUP BY a.trip_id);

SELECT * FROM public.result_04;

#########################################################  TO MARK ORIGINS

CREATE TABLE origins_(
	trip_id text,
	origin geometry,
	weight_class int,
	ping_freq double precision);
	
(SELECT * FROM final_waypoints_ WHERE waypoint_seq = 0);

	
INSERT INTO origins_(trip_id, origin, weight_class, ping_freq) 
(SELECT	a.trip_id, CAST(a.geom as geometry)as origin, a.wt_cls as weight_class, a.ping_freq 
FROM (SELECT * FROM public.final_waypoints_ WHERE waypoint_seq = 0) as a);
															

SELECT * FROM origins_;

#########################################################  TO MARK DESTINATIONS

CREATE TABLE destinations_(
	trip_id text,
	destination geometry,
	waypoint_marker text);

INSERT INTO destinations_(trip_id, destination, waypoint_marker) 
(SELECT	a.trip_id, CAST(a.geom as geometry)as destination, a.wp_seq as waypoint_marker 
FROM (SELECT * FROM public."ODTable_04" WHERE wp_seq = 'Destination') as a);

SELECT * FROM public.destinations_

#########################################################  TO MAKE COMBINED TABLE

CREATE TABLE allTables_(
	trip_id text,
	wt_cls int,
	trip_type int,
	waypoint_seq int,
	geom geometry);
	

INSERT INTO allTables_(trip_id, wt_cls, trip_type, waypoint_seq, geom)
(SELECT trip_id,wt_cls,trip_type, waypoint_seq,geom FROM public.final_waypoints_
UNION
SELECT trip_id,wt_cls,trip_type, waypoint_seq,geom FROM public.final_waypoints_04)

SELECT * FROM allTables_

#########################################################  TO MAKE COMBINED TABLE - latest


SELECT * FROM allTables_

CREATE TABLE origins_(
	trip_id text,
	origin geometry,
	weight_class int);

INSERT INTO origins_(trip_id, origin, weight_class) 
(SELECT	a.trip_id, CAST(a.geom as geometry)as origin, a.wt_cls as weight_class 
FROM (SELECT * FROM public.final_waypoints_ WHERE waypoint_seq = 0) as a);
															
SELECT * FROM origins_;