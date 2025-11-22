
Okay, great work. Now I need to implement some more functionality into this full workflow. These are various APIs that are going to be located in various files, but I want to have their full workflow integrated into this. These are mainly tradin endpoints 







I need help with fixing the functionality of some of my previously refactored files in order to work with the new database files and manager files. The first section that I want to focus on is my markets section. I have a setup.py file in which I call the terminal command python setup.py --load-markets and in this section I am using the Polymarket API to load in all of the available markets from Polymarket into my database in the way that I like. This one is slightly different from most of my other sections, as it relies on the existence of my events data to pull those markets from those events. All of this functionality is currently setup in my various fetch and store files, I just need to make all of the previous functionality work with the new functionality that I have setup for my storage files and database files. I have a database schema file in which I have the database schema defined, and in this file I have a markets table that I have already run. 

The main thing that I need help with is matching the functionality of the markets manager to the relative functionality of the events manager. Notice how with the events manager it mainly acts as an aggregator for calling the fetch functions and storage functions and communicating with the database files as well, and notice how it matches the existing functions in all of these files. This is how I want you to fix the markets manager. With the markets I have the functionality in the fetch functions setup to where it will get the markets using the events existing in the database currently, and then load it into the database. I need you to do the same thing for the storage files as well. I ahve uploaded all of my relevant files for teh database, storage and fetch, and examples from the events to compare as well. Please implement this functionality into my markets files and make sure the database files are up to date as well.












I need help with fixing up some functionality within my database schema and storing functionality for retrieving markets in my Polymarket API setup. 

What I need to do is fix up for one the markets manager file so that it is compatible with the new locations of my fetch, database, and storage files. This file needs to act as an aggregator for the rest of my files and should control this section. 

The main issue that I am facing currently is that when I am loading in my events from the events calling section, I am saving the event data, the tags located in them, and the event tags tables. I am saving to those tables. However, when I call the events there are also the markets that appear in the event object, but in the current event setup workflow, I am not saving the markets that come from the event objects. This means that in the current setup of my load markets workflow, where it tries to call the market ID to see the markets, I am actually calling the event ID into the market API which does not work. In order to fix this issue, I want to do something with both my setup file and markets workflow. What I mean is that in order to get the markets, I want to use the events file to call each event by ID of the events that are already stored in teh database, and using each of these events, the markets will be called and I want to have all of the data stored into the database for the markets table. This is the functionality that I want you to implement, I want to implement into the markets manager the functionality that will allow it to communicate with my fetch and storage and database files, and mainly I want to fix my load markets setup workflow to do what I have described, which would mean I would need to do some calling of the event files to get this done, and then have this data loaded into the database and all of the corresponding tables.

Another thing that I really need to upgrade is my database schema file. This is because before I do anything, I run the command "python setup.py --setup" where the database is created and is setup with the tables. When I am getting the markets, there are a ton of extra tables and embedded data with each market, and I want to capture all of it. Here is what the output of one event call should look like:


I have uploaded all of teh relevant code and pasted all of these paths as well:

setup.py
backend/config.py
backend/events_manager.py
backend/markets_manager.py
backend/fetch/entity/id/id_events.py
backend/fetch/entity/id/id_markets.py
backend/fetch/entity/batch/batch_events.py
backend/fetch/entity/batch/batch_markets.py
backend/database/entity/store_events.py
backend/database/entity/store_markets.py
backend/database/data_fetcher.py
backend/database/database_manager.py
backend/database/database_schema.py
backend/run.py



DO NOT WRITE ANY FUCKING DOCUMENTATION FILES THOSE ARE USELESS AND I DO NOT NEED THEM JUST IMPLEMENT THE FIXES THAT I HAVE ASKED FOR AND SIMPLY JUST SHOW WHAT I NEED TO DO TO RUN THEM!!! NO WRITING CUSTOM SCRIPTS TO FIX IT, NO IMPLEMENTATION GUIDES OR READMES OR EXAMPLES OR ANY OTHER CUTE SHIT!








More things that I want to do with this tags manager file is to refactor some of the existing functionality from my events file and markets file to make it to where the existing functionality in these files regarding fetching tags will be placed back into the tags manager. For the events functionality, this means moving the functions in the events_manager file of 




















I need help with fixing the functionality of my newly refactored application which is getting data from the Polymarket API and loading it into a database. 

What I need to do is fix up for one the tags manager and series manager file so that it is compatible with the new locations of my fetch, database, and storage files. This file needs to act as an aggregator for the rest of my files and should control this section. I want to implement into the tags manager and series manager the functionality that will allow it to communicate with my fetch and storage and database files and then have this data loaded into the database and all of the corresponding tables.

For the tags functionality, I have 4 files which control the fetching of the tags. For this one, I only want to use the fetch tag relationships batch so I can fill my database with tag relationships. I have uploaded another one of my files which is my events manager file for context as to what this file should look like as the tags manager. I have also uploade dthe btach tags and ID tags. I want these tags and their details stored in the tag relationships table with all of its relevant data. I have also uploaded by store_tags file that needs to be fixed up for this new functionality. I have uploaded my store events file as well for context as to what this file should look like. I have pasted below the current database schema for the tag relationships. I have also pasted below the API response from calling the API to get tag relationships. In order to get all of the tag relationships, I have an already existing table of tags that I want to be pulled from and then the response should be the data that will be stored into the tag relationships. We will have to use the event ID's that are in the events table to call the api, and then that response will be stored. 

[
  {
    "id": "<string>",
    "tagID": 123,
    "relatedTagID": 123,
    "rank": 123
  }
]

    -- Tag relationships table
    CREATE TABLE IF NOT EXISTS tag_relationships (
        tag_id TEXT,
        related_tag_id TEXT,
        relationship_type TEXT,
        strength REAL DEFAULT 1.0,
        created_at TEXT,
        PRIMARY KEY (tag_id, related_tag_id),
        FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
    );


For the series functionality, I again have 4 files which control the fetching of the series. Again I want to use the batch series file and the function inside of it to control the mass fetching of the series. I want that data to be saved in the series table. It should also be noted that for each series, it will contain events that are in a series. While I have already downloaded the events, I do not want to download duplicate events, but I do want to keep note as to what events are located in which series. To do this, I have a table called series_events which I want to have all of the events that are contained in a series to be stored in. To do this, I want to have the series ID be the primary key, and then I want to have the any events that are found within a series to have their IDs be in nested JSON object within the column of the database and all of those events linked to the series will be in that one row inside that nested JSON object in one column. This means that you will have to slightly tweak the database schema file to match the description of what I want. I want the exact same thing to happen with the other tables that appear inside a series object as well. This includes series_categories, series_collections, series_chats, and series_tags. I have pasted all of their schemas below. Below I have also pasted what a call to getting this series should look like as in what the response from the API will be. Note all of the functionality that I have told you to implement should ignore the things that are in these objects that I have not told you to implement.


[
  {
    "id": "<string>",
    "ticker": "<string>",
    "slug": "<string>",
    "title": "<string>",
    "subtitle": "<string>",
    "seriesType": "<string>",
    "recurrence": "<string>",
    "description": "<string>",
    "image": "<string>",
    "icon": "<string>",
    "layout": "<string>",
    "active": true,
    "closed": true,
    "archived": true,
    "new": true,
    "featured": true,
    "restricted": true,
    "isTemplate": true,
    "templateVariables": true,
    "publishedAt": "<string>",
    "createdBy": "<string>",
    "updatedBy": "<string>",
    "createdAt": "2023-11-07T05:31:56Z",
    "updatedAt": "2023-11-07T05:31:56Z",
    "commentsEnabled": true,
    "competitive": "<string>",
    "volume24hr": 123,
    "volume": 123,
    "liquidity": 123,
    "startDate": "2023-11-07T05:31:56Z",
    "pythTokenID": "<string>",
    "cgAssetName": "<string>",
    "score": 123,
    "events": [
      {
        "id": "<string>",
        "ticker": "<string>",
        "slug": "<string>",
        "title": "<string>",
        "subtitle": "<string>",
        "description": "<string>",
        "resolutionSource": "<string>",
        "startDate": "2023-11-07T05:31:56Z",
        "creationDate": "2023-11-07T05:31:56Z",
        "endDate": "2023-11-07T05:31:56Z",
        "image": "<string>",
        "icon": "<string>",
        "active": true,
        "closed": true,
        "archived": true,
        "new": true,
        "featured": true,
        "restricted": true,
        "liquidity": 123,
        "volume": 123,
        "openInterest": 123,
        "sortBy": "<string>",
        "category": "<string>",
        "subcategory": "<string>",
        "isTemplate": true,
        "templateVariables": "<string>",
        "published_at": "<string>",
        "createdBy": "<string>",
        "updatedBy": "<string>",
        "createdAt": "2023-11-07T05:31:56Z",
        "updatedAt": "2023-11-07T05:31:56Z",
        "commentsEnabled": true,
        "competitive": 123,
        "volume24hr": 123,
        "volume1wk": 123,
        "volume1mo": 123,
        "volume1yr": 123,
        "featuredImage": "<string>",
        "disqusThread": "<string>",
        "parentEvent": "<string>",
        "enableOrderBook": true,
        "liquidityAmm": 123,
        "liquidityClob": 123,
        "negRisk": true,
        "negRiskMarketID": "<string>",
        "negRiskFeeBips": 123,
        "commentCount": 123,
        "imageOptimized": {
          "id": "<string>",
          "imageUrlSource": "<string>",
          "imageUrlOptimized": "<string>",
          "imageSizeKbSource": 123,
          "imageSizeKbOptimized": 123,
          "imageOptimizedComplete": true,
          "imageOptimizedLastUpdated": "<string>",
          "relID": 123,
          "field": "<string>",
          "relname": "<string>"
        },
        "iconOptimized": {
          "id": "<string>",
          "imageUrlSource": "<string>",
          "imageUrlOptimized": "<string>",
          "imageSizeKbSource": 123,
          "imageSizeKbOptimized": 123,
          "imageOptimizedComplete": true,
          "imageOptimizedLastUpdated": "<string>",
          "relID": 123,
          "field": "<string>",
          "relname": "<string>"
        },
        "featuredImageOptimized": {
          "id": "<string>",
          "imageUrlSource": "<string>",
          "imageUrlOptimized": "<string>",
          "imageSizeKbSource": 123,
          "imageSizeKbOptimized": 123,
          "imageOptimizedComplete": true,
          "imageOptimizedLastUpdated": "<string>",
          "relID": 123,
          "field": "<string>",
          "relname": "<string>"
        },
        "subEvents": [
          "<string>"
        ],
        "markets": [
          {
            "id": "<string>",
            "question": "<string>",
            "conditionId": "<string>",
            "slug": "<string>",
            "twitterCardImage": "<string>",
            "resolutionSource": "<string>",
            "endDate": "2023-11-07T05:31:56Z",
            "category": "<string>",
            "ammType": "<string>",
            "liquidity": "<string>",
            "sponsorName": "<string>",
            "sponsorImage": "<string>",
            "startDate": "2023-11-07T05:31:56Z",
            "xAxisValue": "<string>",
            "yAxisValue": "<string>",
            "denominationToken": "<string>",
            "fee": "<string>",
            "image": "<string>",
            "icon": "<string>",
            "lowerBound": "<string>",
            "upperBound": "<string>",
            "description": "<string>",
            "outcomes": "<string>",
            "outcomePrices": "<string>",
            "volume": "<string>",
            "active": true,
            "marketType": "<string>",
            "formatType": "<string>",
            "lowerBoundDate": "<string>",
            "upperBoundDate": "<string>",
            "closed": true,
            "marketMakerAddress": "<string>",
            "createdBy": 123,
            "updatedBy": 123,
            "createdAt": "2023-11-07T05:31:56Z",
            "updatedAt": "2023-11-07T05:31:56Z",
            "closedTime": "<string>",
            "wideFormat": true,
            "new": true,
            "mailchimpTag": "<string>",
            "featured": true,
            "archived": true,
            "resolvedBy": "<string>",
            "restricted": true,
            "marketGroup": 123,
            "groupItemTitle": "<string>",
            "groupItemThreshold": "<string>",
            "questionID": "<string>",
            "umaEndDate": "<string>",
            "enableOrderBook": true,
            "orderPriceMinTickSize": 123,
            "orderMinSize": 123,
            "umaResolutionStatus": "<string>",
            "curationOrder": 123,
            "volumeNum": 123,
            "liquidityNum": 123,
            "endDateIso": "<string>",
            "startDateIso": "<string>",
            "umaEndDateIso": "<string>",
            "hasReviewedDates": true,
            "readyForCron": true,
            "commentsEnabled": true,
            "volume24hr": 123,
            "volume1wk": 123,
            "volume1mo": 123,
            "volume1yr": 123,
            "gameStartTime": "<string>",
            "secondsDelay": 123,
            "clobTokenIds": "<string>",
            "disqusThread": "<string>",
            "shortOutcomes": "<string>",
            "teamAID": "<string>",
            "teamBID": "<string>",
            "umaBond": "<string>",
            "umaReward": "<string>",
            "fpmmLive": true,
            "volume24hrAmm": 123,
            "volume1wkAmm": 123,
            "volume1moAmm": 123,
            "volume1yrAmm": 123,
            "volume24hrClob": 123,
            "volume1wkClob": 123,
            "volume1moClob": 123,
            "volume1yrClob": 123,
            "volumeAmm": 123,
            "volumeClob": 123,
            "liquidityAmm": 123,
            "liquidityClob": 123,
            "makerBaseFee": 123,
            "takerBaseFee": 123,
            "customLiveness": 123,
            "acceptingOrders": true,
            "notificationsEnabled": true,
            "score": 123,
            "imageOptimized": {
              "id": "<string>",
              "imageUrlSource": "<string>",
              "imageUrlOptimized": "<string>",
              "imageSizeKbSource": 123,
              "imageSizeKbOptimized": 123,
              "imageOptimizedComplete": true,
              "imageOptimizedLastUpdated": "<string>",
              "relID": 123,
              "field": "<string>",
              "relname": "<string>"
            },
            "iconOptimized": {
              "id": "<string>",
              "imageUrlSource": "<string>",
              "imageUrlOptimized": "<string>",
              "imageSizeKbSource": 123,
              "imageSizeKbOptimized": 123,
              "imageOptimizedComplete": true,
              "imageOptimizedLastUpdated": "<string>",
              "relID": 123,
              "field": "<string>",
              "relname": "<string>"
            },
            "events": [
              {}
            ],
            "categories": [
              {
                "id": "<string>",
                "label": "<string>",
                "parentCategory": "<string>",
                "slug": "<string>",
                "publishedAt": "<string>",
                "createdBy": "<string>",
                "updatedBy": "<string>",
                "createdAt": "2023-11-07T05:31:56Z",
                "updatedAt": "2023-11-07T05:31:56Z"
              }
            ],
            "tags": [
              {
                "id": "<string>",
                "label": "<string>",
                "slug": "<string>",
                "forceShow": true,
                "publishedAt": "<string>",
                "createdBy": 123,
                "updatedBy": 123,
                "createdAt": "2023-11-07T05:31:56Z",
                "updatedAt": "2023-11-07T05:31:56Z",
                "forceHide": true,
                "isCarousel": true
              }
            ],
            "creator": "<string>",
            "ready": true,
            "funded": true,
            "pastSlugs": "<string>",
            "readyTimestamp": "2023-11-07T05:31:56Z",
            "fundedTimestamp": "2023-11-07T05:31:56Z",
            "acceptingOrdersTimestamp": "2023-11-07T05:31:56Z",
            "competitive": 123,
            "rewardsMinSize": 123,
            "rewardsMaxSpread": 123,
            "spread": 123,
            "automaticallyResolved": true,
            "oneDayPriceChange": 123,
            "oneHourPriceChange": 123,
            "oneWeekPriceChange": 123,
            "oneMonthPriceChange": 123,
            "oneYearPriceChange": 123,
            "lastTradePrice": 123,
            "bestBid": 123,
            "bestAsk": 123,
            "automaticallyActive": true,
            "clearBookOnStart": true,
            "chartColor": "<string>",
            "seriesColor": "<string>",
            "showGmpSeries": true,
            "showGmpOutcome": true,
            "manualActivation": true,
            "negRiskOther": true,
            "gameId": "<string>",
            "groupItemRange": "<string>",
            "sportsMarketType": "<string>",
            "line": 123,
            "umaResolutionStatuses": "<string>",
            "pendingDeployment": true,
            "deploying": true,
            "deployingTimestamp": "2023-11-07T05:31:56Z",
            "scheduledDeploymentTimestamp": "2023-11-07T05:31:56Z",
            "rfqEnabled": true,
            "eventStartTime": "2023-11-07T05:31:56Z"
          }
        ],
        "series": [
          {}
        ],
        "categories": [
          {
            "id": "<string>",
            "label": "<string>",
            "parentCategory": "<string>",
            "slug": "<string>",
            "publishedAt": "<string>",
            "createdBy": "<string>",
            "updatedBy": "<string>",
            "createdAt": "2023-11-07T05:31:56Z",
            "updatedAt": "2023-11-07T05:31:56Z"
          }
        ],
        "collections": [
          {
            "id": "<string>",
            "ticker": "<string>",
            "slug": "<string>",
            "title": "<string>",
            "subtitle": "<string>",
            "collectionType": "<string>",
            "description": "<string>",
            "tags": "<string>",
            "image": "<string>",
            "icon": "<string>",
            "headerImage": "<string>",
            "layout": "<string>",
            "active": true,
            "closed": true,
            "archived": true,
            "new": true,
            "featured": true,
            "restricted": true,
            "isTemplate": true,
            "templateVariables": "<string>",
            "publishedAt": "<string>",
            "createdBy": "<string>",
            "updatedBy": "<string>",
            "createdAt": "2023-11-07T05:31:56Z",
            "updatedAt": "2023-11-07T05:31:56Z",
            "commentsEnabled": true,
            "imageOptimized": {
              "id": "<string>",
              "imageUrlSource": "<string>",
              "imageUrlOptimized": "<string>",
              "imageSizeKbSource": 123,
              "imageSizeKbOptimized": 123,
              "imageOptimizedComplete": true,
              "imageOptimizedLastUpdated": "<string>",
              "relID": 123,
              "field": "<string>",
              "relname": "<string>"
            },
            "iconOptimized": {
              "id": "<string>",
              "imageUrlSource": "<string>",
              "imageUrlOptimized": "<string>",
              "imageSizeKbSource": 123,
              "imageSizeKbOptimized": 123,
              "imageOptimizedComplete": true,
              "imageOptimizedLastUpdated": "<string>",
              "relID": 123,
              "field": "<string>",
              "relname": "<string>"
            },
            "headerImageOptimized": {
              "id": "<string>",
              "imageUrlSource": "<string>",
              "imageUrlOptimized": "<string>",
              "imageSizeKbSource": 123,
              "imageSizeKbOptimized": 123,
              "imageOptimizedComplete": true,
              "imageOptimizedLastUpdated": "<string>",
              "relID": 123,
              "field": "<string>",
              "relname": "<string>"
            }
          }
        ],
        "tags": [
          {
            "id": "<string>",
            "label": "<string>",
            "slug": "<string>",
            "forceShow": true,
            "publishedAt": "<string>",
            "createdBy": 123,
            "updatedBy": 123,
            "createdAt": "2023-11-07T05:31:56Z",
            "updatedAt": "2023-11-07T05:31:56Z",
            "forceHide": true,
            "isCarousel": true
          }
        ],
        "cyom": true,
        "closedTime": "2023-11-07T05:31:56Z",
        "showAllOutcomes": true,
        "showMarketImages": true,
        "automaticallyResolved": true,
        "enableNegRisk": true,
        "automaticallyActive": true,
        "eventDate": "<string>",
        "startTime": "2023-11-07T05:31:56Z",
        "eventWeek": 123,
        "seriesSlug": "<string>",
        "score": "<string>",
        "elapsed": "<string>",
        "period": "<string>",
        "live": true,
        "ended": true,
        "finishedTimestamp": "2023-11-07T05:31:56Z",
        "gmpChartMode": "<string>",
        "eventCreators": [
          {
            "id": "<string>",
            "creatorName": "<string>",
            "creatorHandle": "<string>",
            "creatorUrl": "<string>",
            "creatorImage": "<string>",
            "createdAt": "2023-11-07T05:31:56Z",
            "updatedAt": "2023-11-07T05:31:56Z"
          }
        ],
        "tweetCount": 123,
        "chats": [
          {
            "id": "<string>",
            "channelId": "<string>",
            "channelName": "<string>",
            "channelImage": "<string>",
            "live": true,
            "startTime": "2023-11-07T05:31:56Z",
            "endTime": "2023-11-07T05:31:56Z"
          }
        ],
        "featuredOrder": 123,
        "estimateValue": true,
        "cantEstimate": true,
        "estimatedValue": "<string>",
        "templates": [
          {
            "id": "<string>",
            "eventTitle": "<string>",
            "eventSlug": "<string>",
            "eventImage": "<string>",
            "marketTitle": "<string>",
            "description": "<string>",
            "resolutionSource": "<string>",
            "negRisk": true,
            "sortBy": "<string>",
            "showMarketImages": true,
            "seriesSlug": "<string>",
            "outcomes": "<string>"
          }
        ],
        "spreadsMainLine": 123,
        "totalsMainLine": 123,
        "carouselMap": "<string>",
        "pendingDeployment": true,
        "deploying": true,
        "deployingTimestamp": "2023-11-07T05:31:56Z",
        "scheduledDeploymentTimestamp": "2023-11-07T05:31:56Z",
        "gameStatus": "<string>"
      }
    ],
    "collections": [
      {
        "id": "<string>",
        "ticker": "<string>",
        "slug": "<string>",
        "title": "<string>",
        "subtitle": "<string>",
        "collectionType": "<string>",
        "description": "<string>",
        "tags": "<string>",
        "image": "<string>",
        "icon": "<string>",
        "headerImage": "<string>",
        "layout": "<string>",
        "active": true,
        "closed": true,
        "archived": true,
        "new": true,
        "featured": true,
        "restricted": true,
        "isTemplate": true,
        "templateVariables": "<string>",
        "publishedAt": "<string>",
        "createdBy": "<string>",
        "updatedBy": "<string>",
        "createdAt": "2023-11-07T05:31:56Z",
        "updatedAt": "2023-11-07T05:31:56Z",
        "commentsEnabled": true,
        "imageOptimized": {
          "id": "<string>",
          "imageUrlSource": "<string>",
          "imageUrlOptimized": "<string>",
          "imageSizeKbSource": 123,
          "imageSizeKbOptimized": 123,
          "imageOptimizedComplete": true,
          "imageOptimizedLastUpdated": "<string>",
          "relID": 123,
          "field": "<string>",
          "relname": "<string>"
        },
        "iconOptimized": {
          "id": "<string>",
          "imageUrlSource": "<string>",
          "imageUrlOptimized": "<string>",
          "imageSizeKbSource": 123,
          "imageSizeKbOptimized": 123,
          "imageOptimizedComplete": true,
          "imageOptimizedLastUpdated": "<string>",
          "relID": 123,
          "field": "<string>",
          "relname": "<string>"
        },
        "headerImageOptimized": {
          "id": "<string>",
          "imageUrlSource": "<string>",
          "imageUrlOptimized": "<string>",
          "imageSizeKbSource": 123,
          "imageSizeKbOptimized": 123,
          "imageOptimizedComplete": true,
          "imageOptimizedLastUpdated": "<string>",
          "relID": 123,
          "field": "<string>",
          "relname": "<string>"
        }
      }
    ],
    "categories": [
      {
        "id": "<string>",
        "label": "<string>",
        "parentCategory": "<string>",
        "slug": "<string>",
        "publishedAt": "<string>",
        "createdBy": "<string>",
        "updatedBy": "<string>",
        "createdAt": "2023-11-07T05:31:56Z",
        "updatedAt": "2023-11-07T05:31:56Z"
      }
    ],
    "tags": [
      {
        "id": "<string>",
        "label": "<string>",
        "slug": "<string>",
        "forceShow": true,
        "publishedAt": "<string>",
        "createdBy": 123,
        "updatedBy": 123,
        "createdAt": "2023-11-07T05:31:56Z",
        "updatedAt": "2023-11-07T05:31:56Z",
        "forceHide": true,
        "isCarousel": true
      }
    ],
    "commentCount": 123,
    "chats": [
      {
        "id": "<string>",
        "channelId": "<string>",
        "channelName": "<string>",
        "channelImage": "<string>",
        "live": true,
        "startTime": "2023-11-07T05:31:56Z",
        "endTime": "2023-11-07T05:31:56Z"
      }
    ]
  }
]


    -- Series tags relationship table
    CREATE TABLE IF NOT EXISTS series_tags (
        series_id TEXT,
        tag_id TEXT,
        tag_slug TEXT,
        PRIMARY KEY (series_id, tag_id),
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
        FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
    );

    -- Series categories relationship table
    CREATE TABLE IF NOT EXISTS series_categories (
        series_id TEXT,
        category_id TEXT,
        PRIMARY KEY (series_id, category_id),
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
        FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
    );

    -- Series collections relationship table
    CREATE TABLE IF NOT EXISTS series_collections (
        series_id TEXT,
        collection_id TEXT,
        PRIMARY KEY (series_id, collection_id),
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
        FOREIGN KEY (collection_id) REFERENCES collections(id) ON DELETE CASCADE
    );

    -- Series chats relationship table
    CREATE TABLE IF NOT EXISTS series_chats (
        series_id TEXT,
        chat_id TEXT,
        PRIMARY KEY (series_id, chat_id),
        FOREIGN KEY (series_id) REFERENCES series(id) ON DELETE CASCADE,
        FOREIGN KEY (chat_id) REFERENCES chats(id) ON DELETE CASCADE
    );


Lastly, this functionality all needs to communicate with my database schema file, data fetcher file, and setup file, which I have all uploaded. Write these files with all of this functionality implemented



setup.py
backend/config.py
backend/events_manager.py
backend/tags_manager.py
backend/series_manager.py
backend/fetch/entity/id/id_events.py
backend/fetch/entity/id/id_series.py
backend/fetch/entity/id/id_series.py
backend/fetch/entity/batch/batch_events.py
backend/fetch/entity/batch/batch_tags.py
backend/fetch/entity/batch/batch_series.py
backend/database/entity/store_events.py
backend/database/entity/store_tags.py
backend/database/entity/store_series.py
backend/database/database_manager.py




DO NOT WRITE ANY FUCKING DOCUMENTATION FILES THOSE ARE USELESS AND I DO NOT NEED THEM JUST IMPLEMENT THE FIXES THAT I HAVE ASKED FOR AND SIMPLY JUST SHOW WHAT I NEED TO DO TO RUN THEM!!! NO WRITING CUSTOM SCRIPTS TO FIX IT, NO IMPLEMENTATION GUIDES OR READMES OR EXAMPLES OR ANY OTHER CUTE SHIT!
