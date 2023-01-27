-- :name get_markethistory 
-- :result :*
SELECT id, date, market, initialprice, price, high, low, volume, bid, ask
FROM marketshistory
WHERE market = :market AND id > :offset
LIMIT :page_size