-module(mysql_test).

-include_lib("eunit/include/eunit.hrl").
-include_lib("mysql.hrl").

-compile(export_all).

basics_test_() ->
  {setup,
    fun start/0,
    fun stop/1,
    fun basics/1}.

transaction_test_() ->
  {setup,
    fun start/0,
    fun stop/1,
    fun transaction/1}.

start() ->
  mysql_statement:start(),
  {ok, Pid} = mysql_conn:start(#mysql_connection_info{host="localhost",
						      port=3306,
						      user="root",
						      password="",
						      database="erlang_mysql_driver_testdb", 
						      log_fun=fun(_,_,_,_) -> ok end, 
						      encoding=utf8}, fuckitttt),
  Pid.
 
stop(Pid) ->
  case is_process_alive(Pid) of
    true ->
      cleanup_conn(Pid);
    false ->
      cleanup_conn(start())
  end,
  mysql_statement:stop_and_cleanup().

cleanup_conn(Pid) ->
  mysql_statement:prepare(truncate_users, <<"TRUNCATE users">>),
  mysql_conn:execute(Pid, truncate_users, []),
  mysql_conn:stop(Pid).

basics(Pid) ->
  mysql_statement:prepare(insert_user, <<"INSERT INTO users (nickname) VALUES (?)">>),
  mysql_conn:execute(Pid, insert_user, ["James"]),
  mysql_conn:execute(Pid, insert_user, ["James"]),
  {data, #mysql_result{rows=Rows}} = mysql_conn:fetch(Pid, <<"SELECT * FROM users">>),
  [?_assertEqual([1, <<"James">>], lists:nth(1, Rows)),
   ?_assertEqual(2, length(Rows))].

transaction(Pid) ->
  mysql_statement:prepare(insert_user, <<"INSERT INTO users (nickname) VALUES (?)">>),
  mysql_conn:transaction(Pid, fun() ->
    mysql_conn:execute(Pid, insert_user, ["James"]),
    mysql_conn:execute(Pid, insert_user, ["James"])
  end),
  mysql_conn:transaction(Pid, fun() ->
    wrong = mysql_conn:execute(Pid, insert_user, ["James"]), % this should get rolled back
    mysql_conn:execute(Pid, insert_user, ["James"])
  end),
  TxnResult = mysql_conn:transaction(Pid, fun() ->
    mysql_conn:execute(Pid, insert_user, ["James"]),
    mysql_conn:rollback(Pid)
  end),
  {data, #mysql_result{rows=Rows}} = mysql_conn:fetch(Pid, <<"SELECT * FROM users">>),
  TxnResult2 = mysql_conn:transaction(Pid, fun() ->
    mysql_conn:stop(Pid),
    mysql_conn:execute(Pid, insert_user, ["James"])
  end),
  [?_assertEqual([1, <<"James">>], lists:nth(1, Rows)),
   ?_assertEqual(2, length(Rows)),
   ?_assertEqual(aborted, TxnResult),
   ?_assertEqual({aborted, connection_exited}, TxnResult2)].
