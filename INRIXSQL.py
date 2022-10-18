from asyncio.windows_events import NULL
from dataclasses import replace
from gettext import install
from multiprocessing import connection
import os, re, sys, subprocess, psycopg2, csv
from pickle import TRUE
from click import progressbar
from tkinter import N
import pandas as pd
from shapely.geometry import Point, LineString, shape
from shapely import wkb, wkt
from sqlalchemy import create_engine
from datetime import datetime

from alive_progress import alive_bar

from datetime import time

import shapefile as sf
import numpy as np

#import importlib
#importlib.reload(help)


class inrixSQL:

        # constants
        global REMOTE_DBNAME, REMOTE_HOST, REMOTE_USER, REMOTE_PORT, REMOTE_PASSWORD, PG_CREDS
        global conn, cursor, write_path, engine
 
        REMOTE_DBNAME = 'postgres'
        REMOTE_HOST = 'toad.dvrpc.org'
        REMOTE_USER = 'readonly_user'
        REMOTE_PORT = '5432'
        REMOTE_PASSWORD = 'readonlytoad'

        #os.chdir("G:/Shared drives/Freight Planning/Technical Studies/FY2021/INRIX Analysis Tool/queries/inrixQueries")

        write_path = "C:/Users/pchatterjee/Desktop/Working/Lower_Bucks_County_Freight_Study/Data_Output"

        PG_CREDS = "postgresql+psycopg2://postgres:dvrpc@localhost:5432/test" # promit #format : "postgresql + psycopg2://username:password@server:5432/dbname"

        # make separate read and write engines?

        #PG_CREDS = "postgresql://postgres:dvrpc@localhost:5432/postgres"

        #format : "postgresql://username:password@server:5432/dbname"

 ######--------------------------------------------------     F     U     N     C     T     I     O     N     S     ------------------------------------------------------------#####


              
        def __init__(args, database, user, password, host, port):
                args.database = REMOTE_DBNAME
                args.user = REMOTE_USER
                args.password = REMOTE_PASSWORD
                args.host = REMOTE_HOST
                args.port = REMOTE_PORT
   
        
        def connection(database, user, password, host, port):
                conn = psycopg2.connect(database = database, user = user , password = password, host = host, port = port)
                print ("Connection Successful!")
                return conn

        conn = connection(REMOTE_DBNAME, REMOTE_USER, REMOTE_PASSWORD, REMOTE_HOST, REMOTE_PORT)
        cursor = conn.cursor()

        print("This is the current WD..")
        print(os.getcwd())
        print("This is the cursor...")
        print(cursor)
        

        def select_trips(cursor, char, weight_class, origin, destination):

                cursor.execute("""SELECT trip_id, vehicle_weight_class, wp_freq_sec, start_pt_geom, end_pt_geom 
                                FROM public.trips{ch}
                                WHERE vehicle_weight_class >= {wt_cls}
                                AND endpoint_type = '3'
                                AND geospatial_type <> 'EE'
                                        AND (origin_zone = '{o_county}'
                                        OR dest_zone = '{d_county}');""". format(ch = char, wt_cls = weight_class, o_county = origin, d_county = destination))

                tables = cursor.fetchall()       
        
                print("Trips", char, "query executed successfully!")

                print(tables)

                print("01_select_trips completed")
                
                return(tables)
        
        


        def unhexify_coords(table):

                # Converts start and end point geometry from HEX to SHAPELY points

                n = 0
                for p in table.st_geom:
                        table.st_geom[n] = wkb.loads(bytes.fromhex(table.st_geom[n])) # converts start point geometry from hex
                        table.end_geom[n] = wkb.loads(bytes.fromhex(table.end_geom[n])) # converts end point geometry from hex   
                        n = n + 1 

                print("02_unhexify_coords completed")
                
                return(table)
       

        conn.commit()
        
        def read_shapefile (filename):
        
                # Accepts a .shp file and and prints the continuous linestring format for the shapefile

                readFile = sf.Reader(filename)
                features = readFile.shapeRecords()[0]
                first = features.shape.__geo_interface__

                print("Shapefile", filename ,"content read completed successfully!")
                print(shape(first))
                print("03_read_shapefile completed")
                return first

        def points_inside (tbl, boundary):

                # Checks for SHAPELY points within the boundaries read in read_shapefile() and returns a table with those points
 
                result_tbl = []
                id = tuple(tbl.trip_id)
                wtcls = tuple(tbl.wt_class)
                starts = 0
                ends = 0
                k = 0

                temp_id = 0
                temp_wtcls = 0

                with alive_bar(len(id), force_tty=True, title='Computing trip starts within bounds...') as bar:

                        for i in tbl.st_geom:


                                if i.within(shape(boundary)): 
                                        temp_id = id[k]
                                        temp_wtcls = wtcls[k]
                                        result_tbl = result_tbl + [[temp_id, temp_wtcls, 0]] # format : [[trip_id, wt-class, origin]]
                                        starts = starts + 1
                        
                                k = k + 1

                                bar()
  
                        #print(table[k])
                        #else:
                                #print(tbl.trip_id[k])

                        print("No. of trips with starts within boundaries: ", starts, "\n")

                        k = 0

                with alive_bar(len(id), force_tty=True, title='Computing trip ends within bounds.....') as bar:

                        for i in tbl.end_geom:


                                if i.within(shape(boundary)): 
                                        temp_id = id[k]
                                        temp_wtcls = wtcls[k]
                                        result_tbl = result_tbl + [[temp_id, temp_wtcls, 1]] # format : [[trip_is, wt-class, destination]]
                                        ends = ends + 1

                                k = k + 1

                                bar()
  
                        #print(table[k])
                        #else:
                                #print(tbl.trip_id[k])

                print("No. of trips with ends within boundaries: ", ends, "\n")

                print("Total trips under consideration: ", starts + ends)

                print("Table with Trip IDs with either origin and/or destination within the specified boundaries are being calculated...")

                
                result_tbl = pd.DataFrame(result_tbl)

                result_tbl.columns = ['trip_id', 'wt_cls', 'trip_type']

                print("04_points_inside completed")

                return(result_tbl)

        

        def select_waypoints(cursor, char, points):

                temp = tuple(points.trip_id)

                with alive_bar(100, force_tty = True) as bar: # added progress bar here to see why the data is not being pulled for tables_07 and _10

                        cursor.execute("""SELECT trip_id, capture_time, waypoint_seq, geom 
                                                FROM public.waypoints{ch}
                                                WHERE trip_id IN {ids} ;""".format(ch = char, ids = temp))

                        bar()

                tables = cursor.fetchall()

                print("Waypoints", char, "query executed successfully!")

                print("05_select_waypoints completed")
                
                return(tables)
        
        

        def noise_filter(points, threshold):

               Table = points[points["wp_freq"] < threshold]
               all = points.shape[0]
               keep = Table.shape[0]

               print(Table, " ", keep, " observations retained")

               print((keep/all)*100, "%", " of records retained for noise threshold ", threshold, "secs")

               print("06_noise_filter completed")

               return(Table)


        def mark_orig_dest(points):


                #lag = 0
                i = 0

                geoms = tuple(points.geom)
                wp_seq = tuple(points.waypoint_seq)
                id = tuple(points.trip_id)

                temp_geoms = []
                temp_id = []
                temp_seq = []

                mainTables = []

                count = 0       

                for row in id[:-1]:
                
                        nxt = id[i+1]
                
                        if row == nxt:
                       
                                temp_id = id[i]
                                temp_geoms = geoms[i]
                                temp_seq = "Thru"
                                mainTables = mainTables + [[temp_id, temp_seq, temp_geoms]]

                                count = count + 1
                  
                
                        elif row != next:

                                temp_id = id[i]
                                temp_geoms = geoms[i]
                                temp_seq = "Destination"
                                mainTables = mainTables + [[temp_id, temp_seq, temp_geoms]]

                                count = count + 1

                        elif wp_seq[i] == 0 :
                                temp_id = id[i]
                                temp_geoms = geoms[i]
                                temp_seq = "Origin"
                                mainTables = mainTables + [[temp_id, temp_seq, temp_geoms]]

                                count = count + 1 
                       

                        i = i + 1
                                #lag = lag + 1

                outTable = pd.DataFrame(mainTables) 

                outTable.columns = ['trip_id', 'wp_seq', 'geom']

                print(outTable)

                outTable.to_csv("ODTables.csv")

                print("07_mark_orig_dest completed")

                return (outTable)

         
        
        
                        

 ######--------------------------------------------------     A     L     L          C     A     L     L     S     ------------------------------------------------------------#####


               
        selected_trips = []
        trips = []
        selected_points = []
        points = []
        chars = ['_10'] # just add the part after the underscore eg "_04"
        #chars = ['', '_04', '_07', '_10'] # 

        with alive_bar(len(chars), force_tty=True, bar = 'bubbles', title='Computing relevant trip IDs......') as bar:

                for c in chars:
                
                        trips = select_trips(cursor, char = c, weight_class = 2, origin = 'BUCKS', destination = 'BUCKS') # selects trips, 
                        
                        #refer to function definition 'select_trips' and inrix data dictionary to change parameters

                        selected_trips = selected_trips + trips # 'trips' processes only the current table, 'selected_trips' collects all 4 tables
                
                        trips = pd.DataFrame(selected_trips) # changes list to a dataframe with rows and cols

                        trips.columns = ['trip_id', 'wt_class', 'wp_freq', 'st_geom', 'end_geom'] # gives names to dataframe called 'trips'

                        trips = unhexify_coords(trips) # changes the coords from HEX to (lat, long) format for next function to work

                        print(trips) # just printing to see if everything is alright; should see points in (lat, long) format in a dataframe

                        boundaries = read_shapefile("KIPC_WGS84_Boundaries.shp") # reads the study area shapefile. This shapefile is located in the 'inrixQueries' folder.

                        # To update study area, update "KIPC_WGS84_Boundaries.shp" and re-run this whole program. Also take note of the Working Directory

                        print("This is the current WD..")

                        print(os.getcwd()) # This is the current WD. "KIPC_WGS84_Boundaries.shp" MUST be located/saved to this file path.

                        trips = noise_filter(trips, threshold = 59)

                        points_inBounds = points_inside(trips, boundaries) # selects all trips with either origin, destination, or both within boundaries

                        #print(points_inBounds) # prints the list of these points just to be sure everything is OK. Can skip this step.

 #----------------------### !!!!!  ### ISSUE IN THIS NEXT STEP FOR 07 and 10 ### COMMENT 354 to 370 to send points to sql #### !!!!! ###-------------------#

                        # (Uncomment) points = select_waypoints(cursor, char = c, points = points_inBounds) # same as 'trips' and 'selected_trips'

                        #print(points) # printing to make sure everything is fine. Can skip this step

                        # (Uncomment) selected_points = selected_points +  points # 'points' will collect only from current table 
                        
                        #while 'selected_points' will collect from all tables

                        # (Uncomment) points = pd.DataFrame(points) # convert 'points' list to dataframe

                        # (Uncomment) points.columns = ['trip_id', 'capture_time','waypoint_seq', 'geom'] # rename these columns for analysis

                        # (Uncomment) points = points_inBounds.merge(points, on = 'trip_id', how = 'left') # left join with the whole universe of points 

                        # to only subset those points that are of interest to us. 

                        # (Uncomment) print(points) # print 'points' again to make sure we have a smaller number of points relevant to our study

                        #subset_points = noise_filter(points, threshold = 59) # 'noise_filter' firstly calculates 'ping_time', then averages it for all waypoints with the same trip_ID

                        # and then filters to keep only those trip_IDs that satisfy the 'threshold'. 

                        #final_points = subset_points.merge(points, on = 'trip_id', how = 'left') # conducts another left join to keep only trip_IDs that satisfy our 'threshold'

                        #final_points.ping_time = subset_points.ping_freq # sets the 'ping_time' for new table called 'subset_points'

                        #ODTable = mark_orig_dest(final_points)
               
                        bar()


        #selected_trips = pd.DataFrame(selected_trips) # un-comment this line if the computer has processing power to compile all 4 tables at once

        #selected_points = pd.DataFrame(selected_points) # un-comment this line if the computer has processing power to compile all 4 tables at once

        # additionally, run the same process for 'selected_points' as done for points to get the master table. 

        # This way we can bypass the need to run 'compileINRIXSQL'

        engine = create_engine(PG_CREDS) # creates engine to access local postgres database using 'PG_CREDS'. This is used to write the final result to your local system

        print(engine) # print to see that all is fine.

        print(pd.__version__)

        #final_points.to_sql('final_waypoints_', engine, if_exists = 'replace', index=False) # change to filename to 'final_waypoints_04 for trips_04 etc.

        points_inBounds.to_sql('final_waypoints_10', engine, if_exists = 'replace', index=False) # change to filename to 'final_waypoints_04 for trips_04 etc.

        
        #ODTable.to_sql('ODTable_', engine, if_exists = 'replace', index=False)

              
        # after this, if no errors pop up, check local postgres database to see if 

        # the new table has appeared. After this, the workflow flows to SQL. In SQL, we turn these points to route paths and make tables called 'result_'

        # we then read these 'result_' tables to 'compileINRIXSQL' and compile all four route tables into a single master table. 

        # the SQL program to do this is the file "SQL_WP_to_ROUTES.sql" in this same working directory.

        ############### FOR SQL ONLY ############## IMPORTANT!!

        # create a new data base, update the engine and write to sql and then run the CODE BELOW in SQL

        # SQL: create extension postgis; ----run this to add postgis functionality to new database
        
        conn.commit() # commits all data transactions 
