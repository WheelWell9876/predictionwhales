# predictionwhales

https://colab.research.google.com/drive/1vqG_XoYvcGt_vBUZWfqEn_5uqII4hrHJ#scrollTo=-kct-6fZkwJ5

https://github.com/Polymarket/py-clob-client
https://docs.polymarket.com/quickstart/introduction/rate-limits

python setup.py --load-tags
python setup.py --load-events
python setup.py --load-markets
python setup.py --load-series
python setup.py --load-users
python setup.py --load-transactions
python setup.py --load-comments

python setup.py --delete-tags
python setup.py --delete-events
python setup.py --delete-markets
python setup.py --delete-series
python setup.py --delete-users
python setup.py --delete-transactions
python setup.py --delete-comments

python setup.py --delete-tags --load-tags
python setup.py --delete-events --load-events
python setup.py --delete-markets --load-markets
python setup.py --delete-series --load-series
python setup.py --delete-users --load-users
python setup.py --delete-transactions --load-transactions
python setup.py --delete-comments --load-comments

python db_utils.py delete-tx
python db_utils.py status


python cleanup_closed_events.py
python analyze_data.py --db polymarket_terminal.db --output report.json






I need help with setting up functionality with my API endpoints for Polymarket. 
