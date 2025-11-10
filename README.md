# predictionwhales

https://colab.research.google.com/drive/1vqG_XoYvcGt_vBUZWfqEn_5uqII4hrHJ#scrollTo=-kct-6fZkwJ5

https://github.com/Polymarket/py-clob-client
https://docs.polymarket.com/quickstart/introduction/rate-limits




Okay, great work. Now I want to implement the actual API calling functionality to get real data. There is gooing to be one of these things that will work in the background which calls the API, transforms it, and loads it into th database, while the frontend will simply get read the data from the database. When I say working in the background, I mean that I will have websockets listening for live data running 24/7 on a live server, so just keep that in mind that is the end state of this application for later, so just make sure anything you are making now will be compatible with that. 

In order to accomplish this, the first thing that I want to do is setup python backend functionality to setup the initial data read from the API and transforming and loading it into the database. This should also include a way to reset the full database as well and modifying the tables. 

The first thing that I want to get is the events themselves. I have a cell that will run to get all of the events in Polymarket that will basically just get a list of all of the events and their info. I do want to have this functionality to run first to download all of the data initially, and then I want it to run once per day (via cron job), to scan for new events. Next, I have another cell which fetches detailed information about the individual markets themselves. I want to use this one for each market as it will cause less stress with each read. This functionality should also include fetching the tags of an event by their ID, which I have a cell implementing that functionality as well. Following that, I want to do the same exact three things for the markets as well. I have those same three cells for the markets and I want to have the same functionality for this. Another thing for the events, I have another cell that gets the live volume for each event. I want to implement this functionality as well into the individual event calls so it can happen in one function, just note that this is another table in this cell, and it is another API endpoint so it will have to be two different calls. This is the same deal for the open interest on markets, which I have a cell for as well. Another thing for the events is that there is another table called series which the events can be a part of. The events can be part of a series of events so they can be put together into one thing as the series will have a name, id, and other info as well. For the series, this will require another table, but the events that are in the series will be linked back to it as well. I have two cells for the series, one for an initial and daily scan of all series, and then another for each individual series. 

Next I want to do the same thing for the tags. I have a cell that will do the initial scan and daily scan for the tags, a cell that fetches info based on the tag ID. Then there is another table for tag relationships by tag ID, for this one I want to implement it by the the initial and daily scan for the tag relationships, and it does it by ID. I also want to have my search the tag relationships for each tag by ID again to have less stress when doing it for each on individually. 

To implement all of this functionality and work with my flask application, I want to have these functions controlling each of the tables in different files in the backend. One file per big table because there is going to be a lot more functionality implemented. 










📊 Database Statistics:
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    events: 51,259 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    markets: 9,353 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    tags: 1,076 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    series: 543 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    collections: 0 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    event_tags: 297,045 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    market_tags: 0 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    users: 920 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    transactions: 0 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    users: 920 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    transactions: 0 records
2025-11-09 23:20:14,488 - PolymarketDataFetcher - INFO -    user_positions: 0 records
2025-11-09 23:20:14,489 - PolymarketDataFetcher - INFO - ============================================================
