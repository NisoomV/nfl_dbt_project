SELECT DISTINCT
    game_id,
    old_game_id,
    home_team,
    away_team,
    season_type,
    week_number,
    game_date
FROM nfl_raw.play_by_play.pbp_raw
ORDER BY old_game_id