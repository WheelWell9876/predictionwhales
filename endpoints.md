Polymarket endpoints

Comments

regular fetch comments - YES - Comments manager. In comments manager fetch_comments_for_all_events, limit 15 for initial load
    - can be done for events, markets, and series. Need to integrate with websockets
    - fetch_comments_for_all_events
    - fetch_comments_for_all_markets
    - _fetch_and_store_event_comments
    - _fetch_and_store_market_comments
    - _fetch_comments
    - _store_comments

fetch comments by comment ID - NO - Comments manager. not yet
    - fetch_comments_for_specific_entities

Fetch Comments by User Address - NO - Comments manager. not implemented yet
    - _fetch_user_comments
    - _store_user_comments

Fetch comment reactions - KINDA - Comments manager. implemented as a function, but not in the initial load
    - _fetch_comment_reactions
    - _fetch_comments_reactions_parallel
    - _fetch_comment_reactions
    - _store_comment_reactions
    - _store_comment_reactions





EVENTS


Fetch and Store All Active Events from Gamma API - YES - Events manager. Integrated and initial load working. 
    - Need to do websockets integration
    - fetch_all_events
    - _fetch_events_batch

Fetch Detailed Event Information by ID - NO - Events manager. More for the individual event fetching. 
    - Need to make a function that will use this for the websocket live and from gamma for data itself.
    - fetch_event_by_id

Fetch Event Tags by Event ID - YES - Tags manager. Integrated and works in initial load. Need to get it with websockets.
    - fetch_event_tags

Fetch Event Information by Slug - NO - Events manager. Kinda redundant for getting the markets, but still need to build in functionality.

- _store_events 
- _store_event_detailed
- process_all_events_detailed


MARKETS


Fetch and Store All Markets for Active Events - YES - Markets manager. I get all of the active markets, and then clean them later. 
    - Need to implement for websockets as well.
    - fetch_all_markets_from_events
    - _fetch_and_store_event_markets
    - _fetch_markets_for_event

Fetch Detailed Market Information by ID - NO - Markets manager. Have not yet because it just one at a time.
    - fetch_market_by_id
    - fetch_market_by_id_parallel

Fetch Market Tags by Market ID - KINDA - Tags manager. I have not done this unlike I have for the events.
    - fetch_market_tags
    - _store_market_tags

Fetch Market Information by Slug - NO - Markets manager. Again redundant but could be used.

- _store_markets
- _store_market_detailed
- _prepare_market_record
- _prepare_detailed_market_record
- process_all_markets_detailed
- _process_market_detailed
- batch_fetch_markets


TAGS


Fetch and Store Tags - YES - Tags manager. Get all tags initially.
    - fetch_all_tags

Fetch Detailed Tag Information by ID - KINDA - Tags manager. Implemented but not in initial load bc one at a time.
    - fetch_tag_by_id
    - fetch_tag_by_id_parallel

Fetch Tag Information by Slug - NO - Tags manager. Redundant but maybe.

Fetch Tag Relationships by Tag ID - KINDA - Tags manager. Yes but not implemented I believe.
    - fetch_tag_relationships
    - _store_tag_relationships

Fetch Tag Relationships by Slug - NO - Tags manager. Redundant for that.

Fetch Full Details of Related Tags by ID - Kinda - Tags manager. Yes but not implemented.
    - fetch_related_tags_details

Fetch Full Details of Related Tags by Slug - NO - Tags manager. Redundant.


- _store_event_tags
- _store_collection_tags
- _store_event_tags_basic
- _fetch_and_store_event_tags
- _store_tags
- _store_tag_detailed
- _prepare_tag_record
- process_all_tags_detailed
- _fetch_tag_detailed_sequential
- _process_tag_detailed
- batch_fetch_tags
- _get_tag_statistics


SERIES


Fetch All Series Data - YES - Series manager. Gets all at initial.
    - fetch_all_series

Fetch Series by ID - YES - Series manager. Implemented but not in intial load.
    - fetch_series_by_id
    - fetch_series_by_id_parallel

- _store_series_list
- _store_series_detailed
- _prepare_series_record
- _store_series_events
- _store_series_collections
- process_all_series_detailed
- _process_series_detailed
- batch_fetch_series


USERS 

Data-API Core

Fetch Current Positions for Users - YES - Transactions manager. Fully implemented and initial.
    - fetch_user_positions_batch
    - _fetch_user_positions_api
    - _fetch_user_current_positions
    - _store_user_current_positions

    - fetch_comprehensive_whale_data
    - _get_whale_users
    - _bulk_insert_positions
    - enrich_all_whale_users
    - _enrich_single_whale_thread_safe
    - _enrich_whale_user_data
    - batch_enrich_whales


Fetch Trades for Users and Markets - NO - Transactions manager. Not implemented.
    - fetch_user_trades_batch
    - _fetch_user_trades_api
    - _bulk_insert_trades
    - _fetch_market_trades
    - fetch_market_transactions
    - _fetch_user_trades
    - _store_user_trades

Fetch User Activity by Wallet - YES - Transactions manager. Fully implemented and initial.
    - fetch_user_activity_batch
    - _fetch_user_activity_api
    - _bulk_insert_activities
    - _fetch_user_activity
    - _store_user_activity



Fetch Top Holders for First Market - YES - IN USER MANAGER NOT TRANSACTIONS MANAGER. But yes it does for all markets.
    - fetch_top_holders_for_all_markets


    - _fetch_and_filter_market_holders_thread_safe
    - _fetch_and_filter_market_holders
    - _check_whale_criteria_from_holder
    - fetch_top_holders_for_markets
    - identify_whale_users


Fetch Transactions for Top Holders - NO - Transactions manager. Not implemented.
    - fetch_recent_whale_transactions
    - _bulk_insert_transactions

Fetch User Portfolio Values - YES - Transactions manager. Fully implemented and initial.
    - fetch_user_values_batch
    - _fetch_user_value_api
    - _bulk_insert_values
    - _fetch_user_wallet_value

Fetch Closed Positions for Users - KINDA - Transactions manager. Implemented but no data showing.
    - fetch_closed_positions_batch
    - _fetch_closed_positions_api
    - _bulk_insert_closed_positions
    - _fetch_user_closed_positions
    - _store_user_closed_positions





Data-API Misc

Fetch User Trading Statistics - NO - User manager. Not implemented.

Fetch Open Interest for Markets - KINDA - Markets manager. Implemented but not being used in initial.
    - fetch_market_open_interest
    - _store_open_interest
    - fetch_global_open_interest

Fetch Live Volume for Events - Kinda - Events manager. Implemented but not initial.
    - fetch_event_live_volume
    - _store_live_volume

CLOB

Fetch Order Book Data from CLOB - NO - Markets manager. Not implemented. Not even a function yet.

Fetch Market Prices from CLOB - NO - Markets manager. Not implemented.

Get Midpoint Price - NO - Markets manager. Not implemented, not even a cell.

Fetch Price History from CLOB - NO - Markets manager. Not implemented, not even a cell yet.

Fetch Bid-Ask Spreads from CLOB - NO - Markets manager. Not implemented or a cell yet.











Kalshi endpoints

Exchange



Get Exchange Status - YES - Exchange Manager. Cell.

Get Exchange Announcements - YES - Exchange Manager. Cell.

Get Series Fee Changes - YES - Exchange manager. Cell.

Get Exchange Schedule - YES - Exchange manager. Cell.

Get User Data Timestamp - YES - User manager. Cell.



Portfolio

Get Balance - YES - Portfolio manager. Cell.

Get Positions - YES - Portfolio manager. Cell.

Get Settlements - YES - Portfolio manager. Cell.

Get Total Resting Order Value - YES - Portfolio manager. cell.

Get Fills - YES - Portfolio manager. cell.

Get Order Groups - YES - Portfolio manager. cell.

Create Order Group - YES - Portfolio manager. cell.

Get Order Group - YES - Portfolio manager. cell.

Delete Order Group - YES - Portfolio manager. cell.

Reset Order Group - YES - Portfolio manager. cell.

Get Orders - YES - Portfolio manager. cell.

Create Order - YES - Portfolio manager. cell.

Batch Create Orders - YES - Portfolio manager. cell.

Batch Cancel Orders - YES - Portfolio manager. cell.

Get Queue Positions for Orders - YES - Portfolio manager. cell.

Get Order - YES - Portfolio manager. cell.

Cancel Order - YES - Portfolio manager. cell.

Amend Order - YES - Portfolio manager. cell

Decrease Order - YES - Portfolio manager. cell.

Get Order Queue Position - YES - Portfolio manager. cell.



Secrets


Get API Keys - YES - Secrets manager. cell.

Create API Key - YES - Secrets manager. cell.

Generate API Key - YES - Secrets manager. cell.

Delete API Key - YES - Secrets manager. cell.



Search 


Get Tags for Series Categories - YES - Tags manager. cell.

Get Filters for Sports - YES - Sports manager. cell.



MARKETS



Get Market Candlesticks - YES - Markets manager. cell.

Get Trades - YES - Transactions manager. cell.

Get Market Orderbook - YES - Markets manager. cell.

Get Series - YES - Series manager. cell.

Get Series List - YES - Series manager. cell.

Get Markets - YES - Markets manager. cell.

Get Market - YES - Markets manager. cell.



EVENTS



Get Event Candlesticks - YES - Events manager. cell.

Get Events - YES - Events manager. cell.

Get Event - YES - Events manager. cell

Get Multivariate Events - YES - Events manager. cell.

Get Event Metadata - YES - Events manager. cell.

Get Event Forecast Percentile History - YES - Events manager. cell.



Live Data



Get Live Data

Get Multiple Live Data




Incentive Programs


Get Volume Incentives - YES - Markets manager. Cell.



FMC 


Get FCM Orders - YES - Transactions manager. Cell.

Get FCM Positions - YES - Transactions manager. cell.



Structured Targets



Get Structured Targets - YES - Transactions manager. cell.

Get Structured Target - YES - Transactions manager. cell.



Milestone


Get Milestone - YES - Events manager. cell.

Get Milestones - YES - Events manager. cell.



Communications



Get Communications ID - YES - Portfolio manager. cell.

Get RFQs - YES - Portfolio manager. cell.

Create RFQ - YES - Portfolio manager. cell.

Get RFQ - YES - Portfolio manager. cell.

Delete RFQ - YES - Portfolio manager. cell.

Get Quotes - YES - Portfolio manager. cell.

Create Quote - YES - Portfolio manager. cell.

Get Quote - YES - Portfolio manager. cell.

Delete Quote - YES - Portfolio manager. cell.

Accept Quote - YES - Portfolio manager. cell.

Confirm Quote - YES - Portfolio manager. cell.



Collections



Get Multivariate Event Collection - YES - Events manager. cell.

Create Market In Multivariate Event Collection - YES - Events manager. cell.

Get Multivariate Event Collections - YES - Events manager. cell.

Get Multivariate Event Collection Lookup History - YES - Events manager. cell.

Lookup Tickers For Market In Multivariate Event Collection - YES - Events manager. cell.







