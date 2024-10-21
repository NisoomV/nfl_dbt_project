SELECT 
    game_id,
    play_id,
    home_team,
    total_home_epa,
    total_home_rush_epa,
    total_home_pass_epa,
    away_team,
    total_away_epa,
    total_away_rush_epa,
    total_away_pass_epa
FROM nfl_raw.play_by_play.pbp_raw
ORDER BY game_id, play_id