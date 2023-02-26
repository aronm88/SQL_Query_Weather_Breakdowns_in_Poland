-- CTE
WITH  data_raw    AS  (SELECT
                        CONCAT(s.usaf, '-', s.name) AS station,
                        CONCAT(w.year, '/', w.mo, '/', w.da) AS date_str,
                        RANK() over (partition by CONCAT(s.usaf, '-', s.name) order by CONCAT(w.year, '/', w.mo, '/', w.da)) AS observation_no, -- assign an observation number for each station
                        (w.temp - 32) * (5/9) as temp_celc, -- conversion from Fahrenheit to Celsius
                        CASE WHEN w.prcp = 99.99 THEN 0 ELSE w.prcp END AS rain -- conversion of the number 99.99 to 0 (no rainfall)
                      FROM `bigquery-public-data.noaa_gsod.gsod2000` w
                      LEFT JOIN `bigquery-public-data.noaa_gsod.stations` s
                      ON w.stn = s.usaf AND w.wban = s.wban
                      WHERE s.country = 'PL'
                      ORDER BY station, observation_no
                      ),
      data_edited AS  (SELECT
                        station,
                        date_str,
                        observation_no,
                        ROUND(temp_celc, 5) AS temp_celc,
                        rain,

                        CASE
                          WHEN observation_no = 1 THEN NULL
                          WHEN lag(rain,1) over (ORDER BY station, date_str, observation_no) = 0 AND rain > 0 THEN 1
                          ELSE 0
                        END as is_rain_today_and_no_rain_last_day,

                        CASE
                          WHEN observation_no <= 7 THEN NULL
                          WHEN rain > ROUND((SUM(rain) over (PARTITION BY station ORDER BY observation_no ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING))/7, 5) THEN 1
                          ELSE 0
                        END AS is_rain_more_than_7day_rolling_average,

                        CASE
                          WHEN observation_no <= 4 THEN NULL
                          WHEN ROUND(temp_celc, 5) <= ROUND((SUM(temp_celc) over (PARTITION BY station ORDER BY observation_no ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING))/4-5, 5) THEN 1
                          ELSE 0
                        END AS is_temp_less_by_5_than_4_day_rolling_average

                      FROM data_raw
                      ORDER BY station, date_str, observation_no
                      )

-- final query with sum in first row
SELECT
  IFNULL(station, 'TOTAL') AS station_label,
  SUM(CASE
        WHEN  is_rain_today_and_no_rain_last_day = 1 AND
              is_rain_more_than_7day_rolling_average = 1 AND
              is_temp_less_by_5_than_4_day_rolling_average = 1 THEN 1
        ELSE 0
      END
  ) AS breakdown
FROM data_edited
GROUP BY ROLLUP(station)
HAVING breakdown <> 0
ORDER BY breakdown DESC;

