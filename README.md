# predictionwhales

https://colab.research.google.com/drive/1vqG_XoYvcGt_vBUZWfqEn_5uqII4hrHJ#scrollTo=-kct-6fZkwJ5

https://github.com/Polymarket/py-clob-client
https://docs.polymarket.com/quickstart/introduction/rate-limits



I need help with implementing wallet creation functionality for my Polymarket whale tracker. For this application which is a flask app, I am trying to track all of the active markets on Polymarket and log any bets of $1,000 or more to be stored in a database. There is a python client for the CLOB of Polymarket, and I believe that this could be a good way to simply see the prices of all of the active markets. The big issue that I am facing is that I will need a wallet in order to see the CLOB API, but I only want to use dummy wallet(s) without any money because they are just going to be there to observe the transactions. Currently I am doing this in a python notebook of which I can connect to the Gamma API, and the Data API, but with the CLOB api which is the really important one, I cannot do it yet because I have not set it up correct. With this part, I will need to modify some of my cells so that it will include the wallet creation functionality as well as passing information in from the created wallet to a client made by Polymarket for CLOB. This is the repo for this client https://github.com/Polymarket/py-clob-client



Using this client, I want to be able to connect to the CLOB api and start out by testing the connection to see if I can see any recent active orders first. To get the L1 authentication for the CLOB API, I need to pass in the following header:



Header	Required?	Description
POLY_ADDRESS	yes	Polygon address
POLY_SIGNATURE	yes	CLOB EIP 712 signature
POLY_TIMESTAMP	yes	Current UNIX timestamp
POLY_NONCE	yes	Nonce. Default 0

https://docs.polymarket.com/developers/CLOB/authentication



What the Gamma markets api does it gets the structure of all of the existing markets, it is not used to see any trades, but rather to see the active markets available. This includes the main trading markets and all of the options available to be traded for said markets. The main market is called an "event" and each available option for these events are the "markets". I have a very basic implementation of calling the gamma API and a response like this:

import requests

r = requests.get("https://gamma-api.polymarket.com/events?closed=false")

response = r.json()

response





Once we have the event, we then gra all of the markets using a setup like this. Note that some events will have just one market and some will have more:

for market in all_events['16085']['markets']:
  if 'outcomePrices' in market and 'clobTokenIds' in market:
    print(market['id'], market['question'], 'outcomePrices' in market and market['outcomePrices'])
    print('clobTokenIds' in market and market['clobTokenIds'])
    print('======')

516723 Will no Fed rate cuts happen in 2025? ["0", "1"]
["18020161444416260195750543688224692006431301166904957549340226313298366177208", "22815929348267525865179400270560699202940891762662686118783091319501232508008"]
======
516730 Will 7 Fed rate cuts happen in 2025? ["0.0015", "0.9985"]
["106637218023173886895550072700357013102189022015797096007920322441434274793343", "79328870430805014620288089322067795525620481132773427804769291919755554601870"]
======
516724 Will 1 Fed rate cut happen in 2025? ["0", "1"]
["15353185604353847122370324954202969073036867278400776447048296624042585335546", "26444182150998636640530251053223191773083977191941080891981240618283910212190"]
======
516725 Will 2 Fed rate cuts happen in 2025? ["0.305", "0.695"]
["11661882248425579028730127122226588074844109517532906275870117904036267401870", "61032109452954507190234229613640921045186796600337553837681909605067067778814"]
======
516731 Will 6 Fed rate cuts happen in 2025? ["0.0025", "0.9975"]
["35428628589990747181456921610814206632405028717182760412162159083085892664954", "33673334616268013961585301204311121178127515047580027207826322300546567181423"]
======
516726 Will 3 Fed rate cuts happen in 2025? ["0.655", "0.345"]
["17601770442239563289082275181138749951422899442850916335476881677007065139739", "39979393291433838641824861631885750592113619894259619599511650636783179688654"]
======
516727 Will 4 Fed rate cuts happen in 2025? ["0.017", "0.983"]
["89004595068776945908481855030043950109477136860300352348370395226706216458498", "66497712136360061394637670796773281770486705493678163278744913561923724550407"]
======
516728 Will 5 Fed rate cuts happen in 2025? ["0.005", "0.995"]
["302273160494790559492931429044277112497487176914839291053945756072421867571", "71168072025438256100937333610884784661496543167790290367141049156834121296266"]
======
516729 Will 8+ Fed rate cuts happen in 2025? ["0.0015", "0.9985"]
["61673387045681238454585997121170806826401631642653090831500827557099057497566", "75673530794317709253317435585138828689432689207295322743064114581203964183879"]
======



I will also want this data to be streaming in real-time so in order to do that I want to use web sockets to do that. I currently have the functionality in place to do this and it looks like the following as an example setup with hard-coded in markets for now (obvious I want to have all of the markets so just make it to where it will get and pass in all of the contracts). But as you can see we can use the individual markets as the things we are streaming from the websockets to get the real-time data, which from the gamma API are the clobTokenIds, but note this is not the same thing as the 0x-prefixed 64-hex string for the markets, that will be important to note later:



import json
import asyncio
import websockets
import datetime

url = 'wss://ws-subscriptions-clob.polymarket.com/ws/market'
last_time_pong = datetime.datetime.now()
msgs = []

fed_2_bps_yes = "61032109452954507190234229613640921045186796600337553837681909605067067778814"
fed_2_bps_no = "11661882248425579028730127122226588074844109517532906275870117904036267401870"

fed_8_bps_yes = "61673387045681238454585997121170806826401631642653090831500827557099057497566"
fed_8_bps_no = "75673530794317709253317435585138828689432689207295322743064114581203964183879"

btc_less_than_20_yes = "96180656579082529010130818562805501105870653065317280806936322177392461281296"
btc_less_than_20_no = "17669060580911376882527520521421617329219303654142453382103185951654496084129"

async with websockets.connect(url) as websocket:
  await websocket.send(json.dumps({"assets_ids": [fed_8_bps_yes, fed_8_bps_no], "type":"market"}))

  while True:
    m = await websocket.recv()
    if m != "PONG":
      last_time_pong = datetime.datetime.now()
    d = json.loads(m)
    print(d)
    if last_time_pong + datetime.timedelta(seconds=10) < datetime.datetime.now():
      await websocket.send("PING")
    else:
      msgs.append(d)

[{'market': '0x12b6268efb832fe64bb746b9f50a51245b761d6b170e172b3cd81a7c4f39432b', 'asset_id': '61673387045681238454585997121170806826401631642653090831500827557099057497566', 'timestamp': '1762107375700', 'hash': '38ff6771911950f27462014513c5a3601b3723b3', 'bids': [{'price': '0.001', 'size': '122013.99'}], 'asks': [{'price': '0.999', 'size': '5000917.22'}, {'price': '0.998', 'size': '2000556.05'}, {'price': '0.997', 'size': '500000'}, {'price': '0.996', 'size': '900000'}, {'price': '0.995', 'size': '28.86'}, {'price': '0.977', 'size': '5'}, {'price': '0.976', 'size': '200000'}, {'price': '0.974', 'size': '200000'}, {'price': '0.973', 'size': '400000'}]
{'market': '0x12b6268efb832fe64bb746b9f50a51245b761d6b170e172b3cd81a7c4f39432b', 'price_changes': [{'asset_id': '75673530794317709253317435585138828689432689207295322743064114581203964183879', 'price': '0.931', 'size': '0', 'side': 'BUY', 'hash': 'e4abf2789f4cb948c9bc22322b4eee7393ccc285', 'best_bid': '0.998', 'best_ask': '0.999'}, {'asset_id': '61673387045681238454585997121170806826401631642653090831500827557099057497566', 'price': '0.069', 'size': '0', 'side': 'SELL', 'hash': '6bbfb4730b3234a0f937546ae3092bdbc06e511f', 'best_bid': '0.001', 'best_ask': '0.002'}], 'timestamp': '1762107601150', 'event_type': 'price_change'}
{'market': '0x12b6268efb832fe64bb746b9f50a51245b761d6b170e172b3cd81a7c4f39432b', 'price_changes': [{'asset_id': '75673530794317709253317435585138828689432689207295322743064114581203964183879', 'price': '0.931', 'size': '25', 'side': 'BUY', 'hash': '432acd3f9d0a2f0daf90ebe5ae4b400c0236d8db', 'best_bid': '0.998', 'best_ask': '0.999'}, {'asset_id': '61673387045681238454585997121170806826401631642653090831500827557099057497566', 'price': '0.069', 'size': '25', 'side': 'SELL', 'hash': '94c5113d609ff88e2fcd7d5c177a07aa6184534a', 'best_bid': '0.001', 'best_ask': '0.002'}], 'timestamp': '1762107601317', 'event_type': 'price_change'}



One of the main challenges to this system is getting the actual wallets. I do have a way of getting the actual wallets by finding the top holders of a market by passing in the 64 bit market hash with the ox prefix like this:



import requests

url = "https://data-api.polymarket.com/holders"

querystring = {"limit":"100","minBalance":"1","market":"0x5b6E4eF2952398983ccEE7E1EFA0fF0D3cf7B12a"}

response = requests.get(url, params=querystring)

print(response.json())

This then gets a response like this:

[
  {
    "token": "17669060580911376882527520521421617329219303654142453382103185951654496084129",
    "holders": [
      {
        "proxyWallet": "0x2cca0493c8b744f0e69923ce2c88b5a274ef782e",
        "bio": "",
        "asset": "17669060580911376882527520521421617329219303654142453382103185951654496084129",
        "pseudonym": "Parallel-Gripper",
        "amount": 12000,
        "displayUsernamePublic": true,
        "outcomeIndex": 0,
        "name": "xiangxaoxuan",
        "profileImage": "",
        "profileImageOptimized": ""
      },
      {
        "proxyWallet": "0x99a18b3de03fc7bf03bfbfb13f827ef6d018b0c3",
        "bio": "",
        "asset": "17669060580911376882527520521421617329219303654142453382103185951654496084129",
        "pseudonym": "Full-Mimosa",
        "amount": 11292.872444,
        "displayUsernamePublic": true,
        "outcomeIndex": 0,
        "name": "BabyYoda12",
        "profileImage": "https://polymarket-upload.s3.us-east-2.amazonaws.com/Baby_Yoda_1024x576_154221a5-039a-4181-9361-910bf2ad6b69_1727623963730.jpg",
        "profileImageOptimized": ""
      },
      {
        "proxyWallet": "0x7bb244d0c70293e66dee84f3d0623fbbbf7d682c",
        "bio": "",
        "asset": "17669060580911376882527520521421617329219303654142453382103185951654496084129",
        "pseudonym": "Belated-Clothes",
        "amount": 10440.04858,
        "displayUsernamePublic": true,
        "outcomeIndex": 0,
        "name": "WongKimArk",
        "profileImage": "https://polymarket-upload.s3.us-east-2.amazonaws.com/profile-image-579181-6fd10a57-f76f-4b08-b878-b30b03176210.png",
        "profileImageOptimized": ""
      }
]

Now that the individual user wallets can be seen, we can then find their on-chain activity using the following implementation:



import requests

url = "https://data-api.polymarket.com/activity"

querystring = {"limit":"100","sortBy":"TIMESTAMP","sortDirection":"DESC","user":"0x72b4b094cdab7322f285fad576473aa3a2197eeb"}

response = requests.get(url, params=querystring)

print(response.json())

[
  {
    "proxyWallet": "0x72b4b094cdab7322f285fad576473aa3a2197eeb",
    "timestamp": 1761625535,
    "conditionId": "0xdf8e2dc5860027decbe6164555c3c1c9645c3bd33e16b9dc57ca87125047d4a8",
    "type": "TRADE",
    "size": 500,
    "usdcSize": 215,
    "transactionHash": "0x400e11b088e34c7efea22211d13f31c89315bb90c82ba7e11cd7617effe13c48",
    "price": 0.43,
    "asset": "79191939610100241429039499950443680906623179487184628479206155805558220344190",
    "side": "BUY",
    "outcomeIndex": 1,
    "title": "Will Luiz Inácio Lula da Silva win the 2026 Brazilian presidential election?",
    "slug": "will-luiz-incio-lula-da-silva-win-the-2026-brazilian-presidential-election",
    "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/will-luiz-incio-lula-da-silva-win-the-2026-brazilian-presidential-election-reTil6nEVB1J.jpg",
    "eventSlug": "brazil-presidential-election",
    "outcome": "No",
    "name": "McMurphy",
    "pseudonym": "Intent-Equivalent",
    "bio": "",
    "profileImage": "",
    "profileImageOptimized": ""
  },
  {
    "proxyWallet": "0x72b4b094cdab7322f285fad576473aa3a2197eeb",
    "timestamp": 1761568575,
    "conditionId": "0xe3afb091ba0c2bd1d0660ac9256f37e0c7560b02eee15641c6cce6ceb62f3769",
    "type": "TRADE",
    "size": 1502.85,
    "usdcSize": 242.38656,
    "transactionHash": "0xeab5c8fa711600809e1fce2841a785dc9de4161a2510f74d820cea937ff4ad0a",
    "price": 0.16128459926140334,
    "asset": "92013558332325676522644286533005478701382407428245344466165291954930219735076",
    "side": "SELL",
    "outcomeIndex": 0,
    "title": "Will Max Verstappen be the 2025 Drivers Champion?",
    "slug": "will-max-verstappen-be-the-2025-drivers-champion",
    "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/will-max-verstappen-be-the-2025-drivers-champion-uieYY6FOz8CW.png",
    "eventSlug": "f1-drivers-champion",
    "outcome": "Yes",
    "name": "McMurphy",
    "pseudonym": "Intent-Equivalent",
    "bio": "",
    "profileImage": "",
    "profileImageOptimized": ""
  }
]

This is how I want to keep a log of all of the users with large transaction amounts. We can keep all transactions over $1,000 for now and maybe dumb it down to $100 later. 



Back to one of the original issues is that I am going to need authentication in order to make trades in the first place, so I need to setup a wallet and pass in the aforementioned headers.



What I want for you to do is to implement the functionality to make this process be used for all markets and not just the ones that I have hard-coded in. Please note that this is all happening in a Google colab file. The first thing that needs to happen is the .env file needs to be setup including the keys and headers setup programmatically from the cell and they will be environment variables that need to be made. This .env file will be written to /content/.env in the console. Once the env file is written, I want to have the gamma markets API called in order to get all available active events. Note that the way to get the active markets uses this URL to not show any inactive markets: 

https://gamma-api.polymarket.com/events?closed=false

These are the parameters for the gamma API request:



limit
integer
Required range: x >= 0
​
offset
integer
Required range: x >= 0
​
order
string
Comma-separated list of fields to order by

​
ascending
boolean
​
id
integer[]
​
slug
string[]
​
tag_id
integer
​
exclude_tag_id
integer[]
​
related_tags
boolean
​
featured
boolean
​
cyom
boolean
​
include_chat
boolean
​
include_template
boolean
​
recurrence
string
​
closed
boolean
​
start_date_min
string<date-time>
​
start_date_max
string<date-time>
​
end_date_min
string<date-time>
​
end_date_max
string<date-time>

Once all of the events have been logged, I want to then do the gamma API for the markets of each of these events and log all of that information as well. There are teh query parameters for the markets gamma call:

limit
integer
Required range: x >= 0
​
offset
integer
Required range: x >= 0
​
order
string
Comma-separated list of fields to order by

​
ascending
boolean
​
id
integer[]
​
slug
string[]
​
clob_token_ids
string[]
​
condition_ids
string[]
​
market_maker_address
string[]
​
liquidity_num_min
number
​
liquidity_num_max
number
​
volume_num_min
number
​
volume_num_max
number
​
start_date_min
string<date-time>
​
start_date_max
string<date-time>
​
end_date_min
string<date-time>
​
end_date_max
string<date-time>
​
tag_id
integer
​
related_tags
boolean
​
cyom
boolean
​
uma_resolution_status
string
​
game_id
string
​
sports_market_types
string[]
​
rewards_min_size
number
​
question_ids
string[]
​
include_tag
boolean
​
closed
boolean

I want to do the same thing for the tags and sports calls for the gamma API. All of these will be separate tables in an SQLite database that I want to be setup here as well:



import requests

url = "https://gamma-api.polymarket.com/tags"

response = requests.get(url)

print(response.json())

limit
integer
Required range: x >= 0
​
offset
integer
Required range: x >= 0
​
order
string
Comma-separated list of fields to order by

​
ascending
boolean
​
include_template
boolean
​
is_carousel
boolean

import requests

url = "https://gamma-api.polymarket.com/teams"

response = requests.get(url)

print(response.json())

limit
integer
Required range: x >= 0
​
offset
integer
Required range: x >= 0
​
order
string
Comma-separated list of fields to order by

​
ascending
boolean
​
league
string[]
​
name
string[]
​
abbreviation
string[]

Once all of the data from the gamma API has been logged. I then want to go ahead and get the first available active market and start listening with that as a web socket using the implementation I showed earlier. 



Once that is done, I want to call the data API for 2 separate things. The first thing I want to do with the data API is to find the top holders of the first available market and log all of their data. Using their wallet address, I then want to get their transactions using the activity API I showed earlier. 



Please write all of these parts as cells in this order. I want the websocket to be in a cell by itself because I will only want it to run for 3 minutes then stop listening. Note that this is being run in Google colab as a notebook, so please make sure it will write any data (including the setup for SQLite and the .env file) into the correct places.



