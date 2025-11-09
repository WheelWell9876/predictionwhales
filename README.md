# predictionwhales

https://colab.research.google.com/drive/1vqG_XoYvcGt_vBUZWfqEn_5uqII4hrHJ#scrollTo=-kct-6fZkwJ5

https://github.com/Polymarket/py-clob-client
https://docs.polymarket.com/quickstart/introduction/rate-limits




I need help with implementing the functionality of a bunch of Polymarket APIs into an interface. The main purpose of this application is to track the top wallets of traders and potentially catch insiders that have consistently hit big trades. But that does NOT concern you for now regarding the implementation of this trading interface.

I want to have different tabs that will functionally represent different entities in the local SQLite database. This is the first thing that I want to do, I want to setup the database schema and the interface itself. For the database, the tables that I need to setup are events, markets, market-tag relationship, tags, tag_relationships, comments, comment_reactions, user_activity, series, series_events, series_collections, collections, series_tags, collection_tags, users, user_positions_current, user_positions_closed, user_trades, user_values, and market_holders. I want to have a for some of these tables individually, but also some of these with the tables combined in the interface. 

For the header, I want it to be black and have the buttons (tabs) light up white when they are hovered over. I want the home button to be in the top left corner, then on the other side I want there to be the login where the user will be shown that they are logged in with their account initials. I want the tabs to be on the header as well which will act as the large navbar, and I want the tab options to be "Trade", "Positions", "Stats", "Whales", and "Settings". Then in the left side of the screen, I wnat an expandable and collapsible navbar which will include options at the top and an area for statistics on the bottom (for that about 75%) of the bottom. 

I want the main first table to be "Users", which will include the users, user_positions_current, user_positions_closed, user_trades, and user_values. This means that I want to mainly have the user, their address abbreviated, any known alias(s), the value of their wallets, and the current positions they have. I want these values to be displayed in a table like it would in a trading terminal with rigid lines (ZERO CURVES, I MEAN ZERO CURVES) like a trading terminal. If the user does not have a known alias, I want to have the person using this terminal to be able to star a user manually and also give them an alias. When a user is selected to be analyzed further, I want the table to be pushed down where the display of the trading view will be shown. For when the user is selected to be analyzed further, I want the candlestick line that is tracking the user to track the value of their wallet. I also want to see any event or market that they interacted with in a buy or sell action. And when that data point is hovered over, I want to have the data point show data of the event of buy or sell and statistics of that event. It is also important to note that within this user table I want to include any comments and comment reactions as well. I want to include backlinks to any comments, markets or events that the user is involved with as well as the links to any transactions. 

The next table that I want to show is the events and markets. This will show the full event which is basically a question and I want this to be a table as well tracking statistics like the volume, market value, and other things that can be derived from this data. When an event is clicked on for further analysis, I want the other events table to be pushed down just like it should for the users., but for the events, I also want those to be pushed down and to the left to make way for the markets as well. The markets are subsets of teh event itself and this is what people actually buy or sell shares on. I will need a creative way of displaying the available markets once an event is clicked. I was thinking three windows below the trading window which shows all of the data because when the market is clicked on, I will want to have relevant data about that shown as well. 

The last two I want to do are the series and tags. I want to have similar setups for both of these as well.

With the user, events, and markets tabs tables, I want to have a candlestick display in the typical other display options for these kind of terminal views. Feel free to use any libraries that are available to be used for these kinds of terminal displays. I want this trading terminal display to include all of the tools that are typically used by traders to draw and markup the display as well. 

I want the graphics of this interface to look very close to a Bloomberg terminal and feel like one as well. I want the background and the vast majority of the site to be black, and I want most of the buttons to match the colors and functionality that would typically looks like a trading terminal.

Here I have pasted the basic tables and some of the basic implementations for the API calls. Note that I want this to be a Flask app, and I want the frontend to be in react. 


