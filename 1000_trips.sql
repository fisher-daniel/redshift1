  select * from rs.t_devices t2, rs.t_policies t1, rs.t_readings t4 (
  select * from rs.t_trips t3 
  where t3.deceleration_1  > 0 
          or t3.acceleration_1 > 0  
       or t3.speeding_1 > 0
          or t3.deceleration_2  > 0 
          or t3.acceleration_2 > 0  
       or t3.speeding_2 > 0
       or t3.deceleration_3  > 0 
          or t3.acceleration_3 > 0  
       or t3.speeding_3 > 0
                      limit 1000) z1
        where z1.device_id = t2.id
        and  z1.policy_id = t1.id
        and z1.id = t4.trip_id;



SELECT z1.id, 
       z1.device_id, 
       z1.policy_id, 
       z1.time_zone, 
       z1.duration, 
       z1.gps_miles, 
       z1.deceleration_1, 
       z1.acceleration_1, 
       z1.speeding_1, 
       z1.deceleration_2, 
       z1.acceleration_2, 
       z1.speeding_2, 
       z1.deceleration_3, 
       z1.acceleration_3, 
       z1.speeding_3, 
       z1.morning, 
       z1.day, 
       z1.evening, 
       z1.night, 
       z1.late_night, 
       z1.rush_hour_am, 
       z1.highest_speed, 
       z1.idle, 
       z1.qos_flags, 
       z1.created_at, 
       t2.make, 
       t2.model, 
       t2.year, 
       t3.city, 
       t3.latitude, 
       t3.longitude, 
       t3.state, 
       t3.zip, 
       z1.start_date, 
       z1.end_date, 
       First_value(t4.latitude) OVER(partition BY trip_id) "start_latitude", 
       Last_value(t4.latitude) OVER(partition BY trip_id) "end_latitude", 
       First_value(t4.longitude) OVER(partition BY trip_id) "start_longitude", 
       Last_value(t4.longitude) OVER(partition BY trip_id) "end_longitude" 
FROM   rs.t_devices t2, 
       rs.t_policies t3, 
       rs.t_readings t4, 
       ( 
              SELECT * 
              FROM   rs.t_trips t3 
              WHERE  t3.deceleration_1 > 0 
              OR     t3.acceleration_1 > 0 
              OR     t3.speeding_1 > 0 
              OR     t3.deceleration_2 > 0 
              OR     t3.acceleration_2 > 0 
              OR     t3.speeding_2 > 0 
              OR     t3.deceleration_3 > 0 
              OR     t3.acceleration_3 > 0 
              OR     t3.speeding_3 > 0 limit 1) z1 
WHERE  z1.device_id = t2.id 
AND    z1.policy_id = t3.id 
AND    z1.id = t4.trip_id;




SELECT trip_id, 
       First_value(latitude) OVER(partition BY trip_id) "start_latitude", 
       Last_value(latitude) OVER(partition BY trip_id) "end_latitude" 
FROM   rs.t_readings t4, 
       ( 
              SELECT * 
              FROM   rs.t_trips t3 
              WHERE  status & 1 = 1 
              AND    duration > 10 
              AND    gps_miles >= 1 
              AND    t3.deceleration_1 > 0 
              OR     t3.acceleration_1 > 0 
              OR     t3.speeding_1 > 0 
              OR     t3.deceleration_2 > 0 
              OR     t3.acceleration_2 > 0 
              OR     t3.speeding_2 > 0 
              OR     t3.deceleration_3 > 0 
              OR     t3.acceleration_3 > 0 
              OR     t3.speeding_3 > 0 limit 100 ) z1 
WHERE  z1.id = t4.trip_id ;



 
 SELECT distinct trip_id, t4.device_id, t4.policy_id, time_zone, duration, gps_miles, deceleration_1, acceleration_1,
 speeding_1, deceleration_2, acceleration_2, speeding_2, deceleration_3, acceleration_3, speeding_3,
 morning, day, evening, night, late_night, rush_hour_am, rush_hour_pm, highest_speed, idle,
 t4.extracted_at, created_at, 
 First_value(latitude) OVER(partition BY trip_id) "start_latitude", 
 Last_value(latitude) OVER(partition BY trip_id) "end_latitude" 
  First_value(longitude) OVER(partition BY trip_id) "start_longitude", 
 Last_value(longitude) OVER(partition BY trip_id) "end_longitude"
FROM   rs.t_readings t4, rs.t_policies t1, rs.t_devices t2
       ( 
              SELECT * 
              FROM   rs.t_trips t3 
              WHERE  status & 1 = 1 
              AND    duration > 10 
              AND    gps_miles >= 1 
              AND    t3.deceleration_1 > 0 
              OR     t3.acceleration_1 > 0 
              OR     t3.speeding_1 > 0 
              OR     t3.deceleration_2 > 0 
              OR     t3.acceleration_2 > 0 
              OR     t3.speeding_2 > 0 
              OR     t3.deceleration_3 > 0 
              OR     t3.acceleration_3 > 0 
              OR     t3.speeding_3 > 0 limit 100 ) z1 
WHERE  z1.id = t4.trip_id 
and t1.id = t4.policy_id
and t2.id = t4.device_id
and latitude > 0
