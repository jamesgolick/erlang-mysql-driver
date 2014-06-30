-module(mysql).

-behaviour(application).

% public api
-export([prepare/2,
	 fetch/2,
	 fetch/3,
	 execute/3,
	 execute/4,
	 transaction/2,
	 transaction/3,
	 rollback/1,
	 rollback/2
	]).

% application callbacks
-export([start/2,
	 stop/1]).

-define(DEFAULT_TIMEOUT, 5000).

start(_Type, _Args) ->
  mysql_sup:start_link().

stop(_State) ->
    ok.

fetch(PoolName, Queries) ->
  fetch(PoolName, Queries, ?DEFAULT_TIMEOUT).

fetch(PoolName, Queries, Timeout) when is_pid(PoolName) ->
  mysql_conn:fetch(PoolName, Queries, Timeout);
fetch(PoolName, Queries, Timeout) when is_atom(PoolName) ->
  poolboy:transaction(PoolName, fun(Worker) ->
	fetch(Worker, Queries, Timeout)
    end).

execute(PoolName, Name, Params) ->
  execute(PoolName, Name, Params, ?DEFAULT_TIMEOUT).


execute(PoolName, Name, Params, Timeout) when is_pid(PoolName) ->
  mysql_conn:execute(PoolName, Name, Params, Timeout);
execute(PoolName, Name, Params, Timeout) when is_atom(PoolName) ->
  poolboy:transaction(PoolName, fun(Worker) ->
	execute(Worker, Name, Params, Timeout)
    end).

transaction(PoolName, Fun) ->
  transaction(PoolName, Fun, ?DEFAULT_TIMEOUT).

transaction(PoolName, Fun, Timeout) when is_pid(PoolName) ->
  mysql_conn:transaction(PoolName, Fun, Timeout);
transaction(PoolName, Fun, Timeout) when is_atom(PoolName) ->
  poolboy:transaction(PoolName, fun(Worker) ->
	transaction(Worker, Fun, Timeout)
    end).

rollback(PoolName) ->
  rollback(PoolName, undefined).

rollback(PoolName, Error) when is_pid(PoolName) ->
  mysql_conn:execute(PoolName, Error);
rollback(PoolName, Error) ->
  poolboy:transaction(PoolName, fun(Worker) ->
	rollback(Worker, Error)
    end).

prepare(Name, Query) ->
  mysql_statement:prepare(Name, Query).
