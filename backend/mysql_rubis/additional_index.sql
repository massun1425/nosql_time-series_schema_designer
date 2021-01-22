-- these indexes are added for faster join queries.
-- Since these indexes are not TPC-H original, the database cannot be benchmarked as TPC-H

CREATE INDEX user_region_hash_index on users(region) using hash;
CREATE INDEX items_category_hash_index on items(category) using hash;
CREATE INDEX items_user_hash_index on items(seller) using hash;
CREATE INDEX bids_user_hash_index on bids(user) using hash;
CREATE INDEX bids_item_hash_index on bids(item) using hash;
CREATE INDEX buynow_buyer_hash_index on buynow(buyer) using hash;
CREATE INDEX buynow_item_hash_index on buynow(item) using hash;
CREATE INDEX comments_from_user_hash_index on comments(from_user) using hash;
CREATE INDEX comments_to_user_hash_index on comments(to_user) using hash;
CREATE INDEX comments_item_hash_index on comments(item) using hash;
