#standardSQL
WITH districts AS (
  SELECT
    CONCAT(county," (",zip_code,")") AS name,
    zcta_geom AS geom,
    geo_id as geo_id
  FROM
    `mlab-sandbox.usa_geo.us_zip_codes`
),
mlab_dl AS (
  SELECT
    tests.*,
    FORMAT(
      '%s_%d',
      ['jun', 'dec'] [ORDINAL(CAST(CEIL(EXTRACT(MONTH FROM partition_date) / 6) AS INT64))],
      EXTRACT(
        YEAR
        FROM
          partition_date
      )
    ) as time_period,
    districts.geo_id AS geo_id
  FROM
    `measurement-lab.release.ndt_downloads` tests
    JOIN districts ON ST_WITHIN(
      ST_GeogPoint(
        connection_spec.client_geolocation.longitude,
        connection_spec.client_geolocation.latitude
      ),
      geom
    )
  WHERE
    connection_spec.server_geolocation.country_name = "United States"
    AND (
      partition_date BETWEEN '2014-07-01'
      AND '2018-12-31'
    )
),
mlab_ul AS (
  SELECT
    tests.*,
    FORMAT(
      '%s_%d',
      ['jun', 'dec'] [ORDINAL(CAST(CEIL(EXTRACT(MONTH FROM partition_date) / 6) AS INT64))],
      EXTRACT(
        YEAR
        FROM
          partition_date
      )
    ) as time_period,
    districts.geo_id AS geo_id
  FROM
    `measurement-lab.release.ndt_uploads` tests
    JOIN districts ON ST_WITHIN(
      ST_GeogPoint(
        connection_spec.client_geolocation.longitude,
        connection_spec.client_geolocation.latitude
      ),
      geom
    )
  WHERE
    connection_spec.server_geolocation.country_name = "United States"
    AND (
      partition_date BETWEEN '2014-07-01'
      AND '2018-12-31'
    )
),
fccdata AS (
  SELECT *, 'dec_2014' AS time_period FROM `mlab-sandbox.fcc.477_dec_2014`
  UNION ALL SELECT *, 'dec_2015' AS time_period FROM `mlab-sandbox.fcc.477_dec_2015`
  UNION ALL SELECT *, 'dec_2016' AS time_period FROM `mlab-sandbox.fcc.477_dec_2016`
  UNION ALL SELECT *, 'jun_2015' AS time_period FROM `mlab-sandbox.fcc.477_jun_2015`
  UNION ALL SELECT *, 'jun_2016' AS time_period FROM `mlab-sandbox.fcc.477_jun_2016`
  UNION ALL SELECT *, 'jun_2017' AS time_period FROM `mlab-sandbox.fcc.477_jun_2017`
  #UNION ALL SELECT *, 'dec_2017' AS time_period FROM `mlab-sandbox.fcc.477_dec_2017`
),
fcc_providerMedians AS (
  SELECT
      time_period,
      APPROX_QUANTILES(CAST(Max_Advertised_Downstream_Speed__mbps_ AS FLOAT64), 101)[SAFE_ORDINAL(51)] AS advertised_down,
      APPROX_QUANTILES(CAST(Max_Advertised_Upstream_Speed__mbps_ AS FLOAT64), 101)[SAFE_ORDINAL(51)] AS advertised_up,
      SUBSTR(Census_Block_FIPS_Code, 0, 5) as geo_id
  FROM fccdata
  WHERE Consumer = '1'
  GROUP BY geo_id, time_period, FRN
),
fcc_groups AS (
  SELECT
      fccdata.time_period,
      COUNT(DISTINCT FRN) AS reg_provider_count, 
      APPROX_QUANTILES(fcc_providerMedians.advertised_down, 101)[SAFE_ORDINAL(51)] AS advertised_down,
      APPROX_QUANTILES(fcc_providerMedians.advertised_up, 101)[SAFE_ORDINAL(51)] AS advertised_up,
      SUBSTR(Census_Block_FIPS_Code, 0, 5) as geo_id
  FROM fccdata JOIN fcc_providerMedians 
  ON SUBSTR(fccdata.Census_Block_FIPS_Code, 0, 5) = fcc_providerMedians.geo_id AND fccdata.time_period = fcc_providerMedians.time_period 
  WHERE Consumer = '1'
  GROUP BY geo_id, time_period 
),
fcc_timeslices AS (
  SELECT ARRAY_AGG(STRUCT(
    time_period,
    reg_provider_count,
    advertised_down,
    advertised_up)) slice,
    geo_id
  FROM fcc_groups
  GROUP BY
  geo_id
)
SELECT
  ARRAY(
    SELECT AS STRUCT
      time_period,
      COUNT(test_id) AS test_count,
      COUNT(DISTINCT connection_spec.client_ip) AS ip_count,
      APPROX_QUANTILES(
        8 * SAFE_DIVIDE(
          web100_log_entry.snap.HCThruOctetsAcked,
          (
            web100_log_entry.snap.SndLimTimeRwin + web100_log_entry.snap.SndLimTimeCwnd + web100_log_entry.snap.SndLimTimeSnd
          )
        ),
        101
      ) [SAFE_ORDINAL(51)] AS tx_Mbps,
      APPROX_QUANTILES(
        CAST(web100_log_entry.snap.MinRTT AS FLOAT64),
        101
      ) [ORDINAL(51)] as min_rtt
    FROM
      mlab_dl
    WHERE mlab_dl.geo_id = districts.geo_id
    GROUP BY
      time_period
  ) mlab_dl,
  ARRAY(
    SELECT AS STRUCT
      time_period,
      COUNT(test_id) AS test_count,
      COUNT(DISTINCT connection_spec.client_ip) AS ip_count,
      APPROX_QUANTILES(
        8 * SAFE_DIVIDE(
          web100_log_entry.snap.HCThruOctetsReceived,
          (
            web100_log_entry.snap.Duration
          )
        ),
        101
      ) [SAFE_ORDINAL(51)] AS tx_Mbps
    FROM
      mlab_ul
    WHERE
      mlab_ul.geo_id = districts.geo_id
    GROUP BY
      time_period
  ) AS mlab_ul,
  fcc_timeslices.* EXCEPT (geo_id),
  districts.geo_id,
  districts.name,
  districts.geom
FROM
  districts JOIN fcc_timeslices USING (geo_id);