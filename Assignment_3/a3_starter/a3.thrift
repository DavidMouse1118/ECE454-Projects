exception IllegalArgument {
  1: string message;
}

service KeyValueService {
  string get(1: string key) throws (1: IllegalArgument e);
  void put(1: string key, 2: string value) throws (1: IllegalArgument e);
  void putBackup(1: string key, 2: string value) throws (1: IllegalArgument e);
  void copyData(1: map<string, string> data) throws (1: IllegalArgument e);
}
