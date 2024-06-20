from typing import Any
import constants
import yaml
import os.path

def _load_yaml_dict(path: str, create_if_not_exists: bool = True):
  def _init_file():
    with open(path, 'w') as file:
      yaml.dump({}, file)

  if not os.path.exists(path) and create_if_not_exists:
    _init_file()
  
  with open(path, 'r') as file:
    data: Any = yaml.safe_load(file)
    if data == None or not isinstance(data, dict) or str(data) == 'None' or str(data).strip() == '':
      _init_file()
      return {}
    
    return data

def _save_yaml_dict(path: str, data: dict):
  with open(path, 'w') as file:
    yaml.dump(data, file)

def get_track_id_db():
  return _load_yaml_dict(constants.TRACK_ID_DB_FILE_NAME);

def get_track_id_overrides_db():
  return _load_yaml_dict(constants.TRACK_ID_DB_OVERRIDES_FILE_NAME);

def set_track_id_db(db_dict: dict):
  return _save_yaml_dict(constants.TRACK_ID_DB_FILE_NAME, db_dict)

def get_missing_tracks_db():
  return _load_yaml_dict(constants.MISSING_TRACKS_FILE_NAME)

def set_missing_tracks_db(db_dict: dict):
  return _save_yaml_dict(constants.MISSING_TRACKS_FILE_NAME, db_dict)