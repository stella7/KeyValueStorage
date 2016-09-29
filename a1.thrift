exception IllegalArgument {
   1: string message;
}

service KeyValueService {
  /* add procedures here */
  list<string> multiGet(1: list<string> keys);
  void multiPut(1: list<string> keys, 2:list<string> values) throws (1: IllegalArgument ia);
}
