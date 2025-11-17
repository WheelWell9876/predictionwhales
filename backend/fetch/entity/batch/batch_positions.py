"""
Batch positions
Handles batch fetching for the positions
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import Dict, List


def fetch_user_positions_batch(self, users: List[str]) -> Dict[str, int]:
        """Fetch current positions for a batch of users using multithreading"""
        self.logger.info(f"Fetching current positions for {len(users)} users...")
        
        total_positions = 0
        users_with_positions = 0
        whale_positions = []
        
        # Collect all positions in memory first
        all_positions = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._fetch_user_positions_api, user): user 
                for user in users
            }
            
            for future in as_completed(futures):
                user = futures[future]
                try:
                    result = future.result()
                    if result['positions']:
                        all_positions.extend(result['positions'])
                        users_with_positions += 1
                        total_positions += len(result['positions'])
                        whale_positions.extend(result['whale_positions'])
                        
                except Exception as e:
                    self.logger.error(f"Error fetching positions for {user}: {e}")
        
        # Now bulk insert all positions at once
        if all_positions:
            self._bulk_insert_positions(all_positions)
        
        # Sort and display top whale positions
        whale_positions.sort(key=lambda x: x['value'], reverse=True)
        
        if whale_positions[:5]:
            self.logger.info("🐋 Top 5 Whale Positions:")
            for pos in whale_positions[:5]:
                self.logger.info(f"   {pos['wallet'][:10]}... - {pos['title'][:40]}")
                self.logger.info(f"     Value: ${pos['value']:,.2f} | P&L: {pos['pnl']:.2%}")
        
        return {
            'users_with_positions': users_with_positions,
            'total_positions': total_positions,
            'whale_positions': len([p for p in whale_positions if p['value'] > 10000])
        }



def fetch_closed_positions_batch(self, users: List[str]) -> Dict[str, int]:
    """Fetch closed positions for a batch of users"""
    self.logger.info(f"Fetching closed positions for {len(users)} users...")
    
    all_positions = []
    big_winners = []
    big_losers = []
    
    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
        # Process in chunks
        chunk_size = 50
        for i in range(0, len(users), chunk_size):
            chunk = users[i:i+chunk_size]
            
            futures = {
                executor.submit(self._fetch_closed_positions_api, user): user 
                for user in chunk
            }
            
            for future in as_completed(futures):
                try:
                    result = future.result()
                    all_positions.extend(result['positions'])
                    big_winners.extend(result['winners'])
                    big_losers.extend(result['losers'])
                except Exception as e:
                    self.logger.debug(f"Error in closed positions batch: {e}")
            
            time.sleep(0.5)  # Small delay between chunks
    
    # Bulk insert positions
    if all_positions:
        self._bulk_insert_closed_positions(all_positions)
    
    # Sort and display top winners/losers
    big_winners.sort(key=lambda x: x['pnl'], reverse=True)
    big_losers.sort(key=lambda x: x['pnl'])
    
    if big_winners[:3]:
        self.logger.info("💰 Top 3 Winning Trades:")
        for win in big_winners[:3]:
            self.logger.info(f"   {win['wallet'][:10]}... - P&L: ${win['pnl']:,.2f}")
    
    if big_losers[:3]:
        self.logger.info("💸 Top 3 Losing Trades:")
        for loss in big_losers[:3]:
            self.logger.info(f"   {loss['wallet'][:10]}... - P&L: ${loss['pnl']:,.2f}")
    
    return {
        'total_positions': len(all_positions),
        'big_winners': len(big_winners),
        'big_losers': len(big_losers)
    }