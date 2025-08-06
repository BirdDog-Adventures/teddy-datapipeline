WITH parcel_boundary AS (
      SELECT
          id as parcel_profile_id,
          parcel_id,
          ST_GeomFromText(parcel_wkt, 4326) as geom,
          ST_Area(ST_Transform(ST_GeomFromText(parcel_wkt, 4326), 3857)) / 4047 as parcel_acres
      FROM parcel_profile
      WHERE id = 'f079e0c6-98db-4cdb-9f68-90801cd8d661'
        AND parcel_wkt IS NOT NULL
        AND deleted_at IS NULL
  ),
  intersecting_mapunits AS (
      SELECT
          pb.parcel_profile_id,
          pb.parcel_id,
          pb.parcel_acres,
          mu.mukey,
          mu.musym as map_unit_symbol,
          mu.areasymbol,
          mu.shape_area as mupolygon_area,
          mu.shape_length as mupolygon_perimeter,
          mu.spatialver,

          -- Pre-convert geometries once for reuse
          pb.geom as parcel_geom,
          ST_GeomFromText(mu.geometry_wkt, 4326) as mu_geom,

          -- Calculate intersection once and reuse
          ST_Intersection(pb.geom, ST_GeomFromText(mu.geometry_wkt, 4326)) as intersection_geom

      FROM parcel_boundary pb
      JOIN ssurgo_mupolygon mu ON ST_Intersects(pb.geom, ST_GeomFromText(mu.geometry_wkt, 4326))
      WHERE mu.geometry_wkt IS NOT NULL
  ),
  mapunit_calculations AS (
      SELECT
          parcel_profile_id,
          parcel_id,
          parcel_acres,
          mukey,
          map_unit_symbol,
          areasymbol,
          mupolygon_area,
          mupolygon_perimeter,
          spatialver,
          parcel_geom,
          mu_geom,

          -- Calculate metrics from pre-computed intersection
          ST_Area(ST_Transform(intersection_geom, 3857)) / 4047 as intersection_acres,
          (ST_Area(ST_Transform(intersection_geom, 3857)) / ST_Area(ST_Transform(parcel_geom, 3857))) * 100 as
  coverage_percent,
          ST_Distance(
              ST_Transform(ST_Centroid(parcel_geom), 3857),
              ST_Transform(ST_Centroid(mu_geom), 3857)
          ) * 0.000621371 as distance_miles
      FROM intersecting_mapunits
      WHERE ST_Area(intersection_geom) > 0
  ),
  component_data AS (
      SELECT
          mc.*,
          COALESCE(map.muname, 'Unknown') as map_unit_name,
          COALESCE(map.mukind, 'Unknown') as map_unit_kind,
          comp.cokey as component_key,
          COALESCE(comp.compname, 'Unknown') as soil_series,
          COALESCE(comp.comppct_r, 100) as component_percentage,
          COALESCE(comp.majcompflag, 'Yes') as is_major_component,
          COALESCE(comp.slope_r, 0) as slope_percent,
          COALESCE(comp.hydgrp, 'Unknown') as hydrologic_group,
          COALESCE(comp.taxclname, 'Unknown') as taxonomic_class,

          -- Confidence score calculation
          CASE
              WHEN mc.coverage_percent >= 75 AND COALESCE(comp.comppct_r, 100) >= 75 THEN 0.95
              WHEN mc.coverage_percent >= 50 AND COALESCE(comp.comppct_r, 100) >= 50 THEN 0.85
              WHEN mc.coverage_percent >= 25 AND COALESCE(comp.comppct_r, 100) >= 25 THEN 0.75
              WHEN mc.coverage_percent >= 10 AND COALESCE(comp.comppct_r, 100) >= 15 THEN 0.65
              ELSE 0.50
          END as confidence_score,

          -- Match quality
          CASE
              WHEN mc.coverage_percent >= 75 AND COALESCE(comp.majcompflag, 'Yes') = 'Yes' THEN 'Excellent'
              WHEN mc.coverage_percent >= 50 AND COALESCE(comp.majcompflag, 'Yes') = 'Yes' THEN 'Good'
              WHEN mc.coverage_percent >= 25 OR COALESCE(comp.majcompflag, 'Yes') = 'Yes' THEN 'Fair'
              ELSE 'Poor'
          END as match_quality

      FROM mapunit_calculations mc
      LEFT JOIN ssurgo_mapunit map ON mc.mukey = map.mukey
      LEFT JOIN ssurgo_component comp ON mc.mukey = comp.mukey
      WHERE comp.majcompflag = 'Yes' OR comp.comppct_r >= 15
  ),
  horizon_data AS (
      SELECT
          cd.*,
          COALESCE(hor.om_r, 2.0) as organic_matter_pct,
          COALESCE(hor.ph1to1h2o_r, 6.5) as ph_level,
          COALESCE(hor.cec7_r, 10.0) as cation_exchange_capacity,
          COALESCE(hor.awc_r, 0.12) as available_water_capacity,
          COALESCE(hor.sandtotal_r, 40.0) as sand_percent,
          COALESCE(hor.silttotal_r, 40.0) as silt_percent,
          COALESCE(hor.claytotal_r, 20.0) as clay_percent,
          COALESCE(hor.pbray1_r, 0) as phosphorus_ppm,
          COALESCE(hor.hzdepb_r, 30) as sampling_depth_cm,

          -- Calculate nutrients
          CASE
              WHEN COALESCE(hor.om_r, 2.0) >= 3.0 THEN COALESCE(hor.om_r, 2.0) * 20
              ELSE COALESCE(hor.om_r, 2.0) * 15
          END as estimated_nitrogen_ppm,

          CASE
              WHEN COALESCE(hor.cec7_r, 10.0) >= 15 THEN COALESCE(hor.cec7_r, 10.0) * 8
              ELSE COALESCE(hor.cec7_r, 10.0) * 6
          END as estimated_potassium_ppm,

          -- Soil texture classification
          CASE
              WHEN COALESCE(hor.claytotal_r, 20.0) >= 40 THEN 'Clay'
              WHEN COALESCE(hor.sandtotal_r, 40.0) >= 70 THEN 'Sandy'
              WHEN COALESCE(hor.silttotal_r, 40.0) >= 50 THEN 'Silty'
              ELSE 'Loam'
          END as soil_texture_class,

          -- Fertility classification
          CASE
              WHEN COALESCE(hor.om_r, 2.0) >= 3.0 AND COALESCE(hor.ph1to1h2o_r, 6.5) BETWEEN 6.0 AND 7.0 THEN 'High'
              WHEN COALESCE(hor.om_r, 2.0) >= 2.0 AND COALESCE(hor.ph1to1h2o_r, 6.5) BETWEEN 5.5 AND 7.5 THEN
  'Medium'
              ELSE 'Low'
          END as fertility_class,

          -- Agricultural capability
          CASE
              WHEN cd.slope_percent <= 6 AND COALESCE(hor.ph1to1h2o_r, 6.5) BETWEEN 6.0 AND 7.5 THEN 'Class I-II - 
  Excellent'
              WHEN cd.slope_percent <= 12 AND COALESCE(hor.ph1to1h2o_r, 6.5) BETWEEN 5.5 AND 8.0 THEN 'Class III - 
  Good'
              WHEN cd.slope_percent <= 18 THEN 'Class IV - Fair'
              ELSE 'Class V+ - Limited'
          END as agricultural_capability

      FROM component_data cd
      LEFT JOIN ssurgo_horizon hor ON cd.component_key = hor.cokey
      WHERE hor.hzdept_r <= 30 OR hor.hzdept_r IS NULL
  )

  INSERT INTO soil_profile (
      parcel_profile_id,
      parcel_id,
      mukey,
      map_unit_symbol,
      component_key,
      soil_series,
      distance_miles,
      confidence_score,
      match_quality,
      soil_type,
      fertility_class,
      organic_matter_pct,
      ph_level,
      cation_exchange_capacity,
      hydrologic_group,
      slope_percent,
      available_water_capacity,
      nitrogen_ppm,
      phosphorus_ppm,
      potassium_ppm,
      taxonomic_class,
      agricultural_capability,
      component_percentage,
      sampling_depth_cm,
      last_updated
  )
  SELECT
      hd.parcel_profile_id,
      hd.parcel_id,
      hd.mukey,
      hd.map_unit_symbol,
      hd.component_key,
      hd.soil_series,
      ROUND(hd.distance_miles::NUMERIC, 3)::TEXT,
      hd.confidence_score,
      hd.match_quality,
      hd.soil_texture_class,
      hd.fertility_class,
      hd.organic_matter_pct,
      hd.ph_level,
      hd.cation_exchange_capacity,
      hd.hydrologic_group,
      hd.slope_percent,
      hd.available_water_capacity,
      hd.estimated_nitrogen_ppm,
      hd.phosphorus_ppm,
      hd.estimated_potassium_ppm,
      hd.taxonomic_class,
      hd.agricultural_capability,
      hd.component_percentage,
      hd.sampling_depth_cm,
      CURRENT_TIMESTAMP
  FROM horizon_data hd
  WHERE hd.is_major_component = 'Yes' OR hd.component_percentage >= 15
  ORDER BY hd.coverage_percent DESC, hd.component_percentage DESC;


  select count(*) from topography;