def ms_meta(entity: str, entity_id: str) -> dict:
    return {"meta": {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/{entity_id}",
                     "type": entity.upper()}}

def ms_state_meta(state_id: str) -> dict:
    return {"meta": {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/customerorder/metadata/states/{state_id}",
                     "type": "state"}}

def ms_demand_state_meta(state_id: str) -> dict:
    return {"meta": {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/demand/metadata/states/{state_id}",
                     "type": "state"}}

def ms_sales_channel_meta(channel_id: str) -> dict:
    return {"meta": {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/saleschannel/{channel_id}",
                     "type": "saleschannel"}}
