from typing import Optional
import requests


def fetch_market_open_interest(self, market_id: str, condition_id: str = None) -> Optional[float]:
        """
        Fetch open interest data for a market
        """
        if not self.config.FETCH_OPEN_INTEREST:
            return None
        
        try:
            url = f"{self.data_api_url}/oi"
            
            # Try with market_id first
            params = {"market": market_id}
            
            response = requests.get(
                url,
                params=params,
                headers=self.config.get_api_headers(),
                timeout=self.config.REQUEST_TIMEOUT
            )
            
            if response.status_code != 200 and condition_id:
                # Try with condition_id if market_id fails
                params = {"market": condition_id}
                response = requests.get(
                    url,
                    params=params,
                    headers=self.config.get_api_headers(),
                    timeout=self.config.REQUEST_TIMEOUT
                )
            
            response.raise_for_status()
            
            data = response.json()
            
            if data and len(data) > 0:
                oi_value = data[0].get('value', 0)
                with self._db_lock:
                    self._store_open_interest(market_id, condition_id, oi_value)
                return oi_value
            
            return None
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching open interest for market {market_id}: {e}")
            return None