CREATE TABLE IF NOT EXISTS candles(
    timestamp INTEGER PRIMARY KEY NOT NULL, 
    bid_open REAL, 
    bid_high REAL, 
    bid_low REAL, 
    bid_close REAL, 
    ask_open REAL, 
    ask_high REAL, 
    ask_low REAL, 
    ask_close REAL, 
    volume INTEGER);

INSERT INTO candles 
    SELECT strftime('%s', strftime('%Y-%m-%d %H:00:00', timestamp/1000, 'unixepoch'))*1000 AS timestamp, 
    (SELECT bid FROM ticks WHERE timestamp > strftime('%s', strftime('%Y-%m-%d %H:00:00', timestamp/1000, 'unixepoch'))*1000 ORDER BY timestamp LIMIT 1) AS bid_open, 
    MAX(bid) AS bid_high, 
    MIN(bid) AS bid_low, 
    (SELECT bid FROM ticks WHERE timestamp < 3600000 + strftime('%s', strftime('%Y-%m-%d %H:00:00', timestamp / 1000, 'unixepoch'))*1000 ORDER BY timestamp DESC LIMIT 1) AS bid_close, 
    (SELECT ask FROM ticks WHERE timestamp > strftime('%s', strftime('%Y-%m-%d %H:00:00', timestamp / 1000, 'unixepoch'))*1000 ORDER BY timestamp LIMIT 1) AS ask_open, 
    MAX(ask) AS ask_high, 
    MIN(ask) AS ask_low, 
    (SELECT ask FROM ticks WHERE timestamp < 3600000 + strftime('%s', strftime('%Y-%m-%d %H:00:00', timestamp / 1000, 'unixepoch'))*1000 ORDER BY timestamp DESC LIMIT 1) AS ask_close, 
    COUNT(id) AS volume FROM ticks GROUP BY strftime('%s', strftime('%Y-%m-%d %H:00:00', timestamp/1000, 'unixepoch'))*1000;
