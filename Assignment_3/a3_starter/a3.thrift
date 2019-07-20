service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void putBackup(1: string key, 2: string value);
  void setPrimary(1: bool isPrimary);
  void copyData(1: map<string, string> data);
}
