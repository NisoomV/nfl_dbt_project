
  create or replace   view nfl_raw.dbt.plays
  
   as (
    SELECT
    play_id,
    game_id,
    posteam,
    defteam,
    side_of_field,
    yardline_100,
    game_half,
    qtr,
    down,
    ydstogo,
    game_time,
    play_desc,
    play_type,
    yards_gained,
    pass_length,
    pass_location,
    air_yards,
    yards_after_catch,
    run_location,
    run_gap,
    posteam_score,
    defteam_score,
    score_differential
FROM nfl_raw.play_by_play.pbp_raw
ORDER BY game_id, qtr, game_time DESC
  );

