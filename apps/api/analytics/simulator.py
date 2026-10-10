"""Transfer simulation adapted from Danh; seeded and isolated from historical data."""
import random
from datetime import datetime, timedelta
from typing import List

PL_CLUBS = [{'code': 'ARS', 'name': 'Arsenal'}, {'code': 'AVL', 'name': 'Aston Villa'}, {'code': 'BOU', 'name': 'Bournemouth'}, {'code': 'BRE', 'name': 'Brentford'}, {'code': 'BHA', 'name': 'Brighton & Hove Albion'}, {'code': 'CHE', 'name': 'Chelsea'}, {'code': 'CRY', 'name': 'Crystal Palace'}, {'code': 'EVE', 'name': 'Everton'}, {'code': 'FUL', 'name': 'Fulham'}, {'code': 'IPS', 'name': 'Ipswich Town'}, {'code': 'LEI', 'name': 'Leicester City'}, {'code': 'LIV', 'name': 'Liverpool'}, {'code': 'MCI', 'name': 'Manchester City'}, {'code': 'MUN', 'name': 'Manchester United'}, {'code': 'NEW', 'name': 'Newcastle United'}, {'code': 'NFO', 'name': 'Nottingham Forest'}, {'code': 'SOU', 'name': 'Southampton'}, {'code': 'TOT', 'name': 'Tottenham Hotspur'}, {'code': 'WHU', 'name': 'West Ham United'}, {'code': 'WOL', 'name': 'Wolverhampton Wanderers'}]

FOREIGN_CLUBS = [{'name': 'Real Madrid', 'league': 'La Liga'}, {'name': 'Barcelona', 'league': 'La Liga'}, {'name': 'Bayern Munich', 'league': 'Bundesliga'}, {'name': 'Bayer Leverkusen', 'league': 'Bundesliga'}, {'name': 'Paris Saint-Germain', 'league': 'Ligue 1'}, {'name': 'Juventus', 'league': 'Serie A'}, {'name': 'Inter Milan', 'league': 'Serie A'}, {'name': 'AC Milan', 'league': 'Serie A'}, {'name': 'Atletico Madrid', 'league': 'La Liga'}, {'name': 'Borussia Dortmund', 'league': 'Bundesliga'}, {'name': 'Sporting CP', 'league': 'Liga Portugal'}, {'name': 'Benfica', 'league': 'Liga Portugal'}, {'name': 'Ajax', 'league': 'Eredivisie'}, {'name': 'RB Leipzig', 'league': 'Bundesliga'}, {'name': 'Atalanta', 'league': 'Serie A'}, {'name': 'Al-Hilal', 'league': 'Saudi Pro League'}, {'name': 'Al-Nassr', 'league': 'Saudi Pro League'}]

PLAYERS_CATALOG = [{'name': 'Florian Wirtz', 'base_val': 130000000.0}, {'name': 'Jamal Musiala', 'base_val': 130000000.0}, {'name': 'Viktor Gyökeres', 'base_val': 75000000.0}, {'name': 'Victor Osimhen', 'base_val': 90000000.0}, {'name': 'Rodrygo', 'base_val': 110000000.0}, {'name': 'Khvicha Kvaratskhelia', 'base_val': 85000000.0}, {'name': 'Martin Zubimendi', 'base_val': 60000000.0}, {'name': 'Alexander Isak', 'base_val': 95000000.0}, {'name': 'Bruno Guimarães', 'base_val': 85000000.0}, {'name': 'Cole Palmer', 'base_val': 115000000.0}, {'name': 'Kobbie Mainoo', 'base_val': 65000000.0}, {'name': 'Michael Olise', 'base_val': 75000000.0}, {'name': 'Benjamin Šeško', 'base_val': 65000000.0}, {'name': 'Nico Williams', 'base_val': 75000000.0}, {'name': 'Xavi Simons', 'base_val': 80000000.0}, {'name': 'Evan Ferguson', 'base_val': 55000000.0}, {'name': 'João Neves', 'base_val': 65000000.0}, {'name': 'António Silva', 'base_val': 50000000.0}, {'name': 'Castello Lukeba', 'base_val': 45000000.0}, {'name': 'Morgan Gibbs-White', 'base_val': 50000000.0}, {'name': 'Eberechi Eze', 'base_val': 60000000.0}, {'name': 'Marc Guéhi', 'base_val': 50000000.0}, {'name': 'Pedro Neto', 'base_val': 60000000.0}, {'name': 'Archie Gray', 'base_val': 35000000.0}, {'name': 'Adam Wharton', 'base_val': 45000000.0}, {'name': 'Leny Yoro', 'base_val': 55000000.0}, {'name': 'Amadou Onana', 'base_val': 55000000.0}, {'name': 'Jarrad Branthwaite', 'base_val': 55000000.0}, {'name': 'Ethan Nwaneri', 'base_val': 25000000.0}, {'name': 'Malo Gusto', 'base_val': 40000000.0}, {'name': 'Arda Güler', 'base_val': 45000000.0}, {'name': 'Endrick', 'base_val': 60000000.0}, {'name': 'Warren Zaïre-Emery', 'base_val': 70000000.0}]

def generate_simulated_transfers(batch_size: int=5, season: str='2025-2026', seed: int=42) -> List[dict]:
    """
    Sinh các giao dịch chuyển nhượng giả lập cho mùa giải tương lai.
    Đảm bảo 100% đúng schema:
    tm_player_id, player_name, transfer_date, from_pl_club, to_pl_club,
    from_club_name, to_club_name, transfer_type, fee_eur, market_value_eur, season
    """
    records = []
    rng = random.Random(seed)
    types_pool = ['Permanent', 'Permanent', 'Permanent', 'Free', 'Internal/academy', 'Unknown fee']
    start_date = datetime(int(season[:4]), 6, 1)
    for _ in range(batch_size):
        player = rng.choice(PLAYERS_CATALOG)
        p_id = str(rng.randint(100000, 999999))
        t_type = rng.choice(types_pool)
        scenario = rng.choice(['foreign_to_pl', 'pl_to_pl', 'pl_to_foreign'])
        pl_source = rng.choice(PL_CLUBS)
        pl_dest = rng.choice([c for c in PL_CLUBS if c['code'] != pl_source['code']])
        foreign = rng.choice(FOREIGN_CLUBS)
        if t_type == 'Internal/academy':
            from_pl = pl_dest['code']
            to_pl = pl_dest['code']
            from_name = f"{pl_dest['name']} U21"
            to_name = pl_dest['name']
            fee = 0.0
            market_val = round(rng.uniform(500000, 15000000), -4)
        elif scenario == 'foreign_to_pl':
            from_pl = ''
            to_pl = pl_dest['code']
            from_name = foreign['name']
            to_name = pl_dest['name']
        elif scenario == 'pl_to_pl':
            from_pl = pl_source['code']
            to_pl = pl_dest['code']
            from_name = pl_source['name']
            to_name = pl_dest['name']
        else:
            from_pl = pl_source['code']
            to_pl = ''
            from_name = pl_source['name']
            to_name = foreign['name']
        if t_type == 'Permanent':
            base = player['base_val']
            market_val = round(base * rng.uniform(0.85, 1.25), -5)
            fee = round(market_val * rng.uniform(0.9, 1.35), -5)
        elif t_type in ['Free', 'Unknown fee']:
            market_val = round(player['base_val'] * rng.uniform(0.7, 1.1), -5)
            fee = 0.0
        day_offset = rng.randint(0, 90)
        t_date = (start_date + timedelta(days=day_offset)).strftime('%Y-%m-%d')
        records.append({'tm_player_id': p_id, 'player_name': player['name'], 'transfer_date': t_date, 'from_pl_club': from_pl, 'to_pl_club': to_pl, 'from_club_name': from_name, 'to_club_name': to_name, 'transfer_type': t_type, 'fee_eur': float(fee), 'market_value_eur': float(market_val), 'season': season})
    return records
