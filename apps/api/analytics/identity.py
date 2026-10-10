"""Conservative DataMB-to-Silver identity links; ambiguous names stay unlinked."""
import re
import unicodedata
from collections import defaultdict

def normalized_name(name):
    name=unicodedata.normalize('NFKD',name)
    return re.sub(r'[^a-z0-9]+',' ',''.join(c for c in name if not unicodedata.combining(c)).casefold()).strip()

def link_players(players, football_data):
    candidates=defaultdict(set)
    for row in football_data.players:
        candidates[(normalized_name(row['player_name']),row['season'])].add(row['player_id'])
    result=[]
    for row in players:
        found=candidates[(normalized_name(row['name']),row['season'])]
        canonical=next(iter(found)) if len(found)==1 else None
        result.append({**row,'canonical_player_id':canonical,'tm_player_id':football_data.bridge.get(canonical),'identity_status':'unique_name_season' if canonical else 'unresolved_or_ambiguous'})
    return result
