export PYTHONPATH=$PYTHONPATH:/feedserver:/
cd /feedserver/binance
source /feedserver/coingecko/env/bin/activate
python liquidation.py
