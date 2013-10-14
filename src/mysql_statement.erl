-module(mysql_statement).

-behaviour(gen_server).

%% External exports
-export([start_link/0,
    start/0,
    stop/0,
    stop_and_cleanup/0,
    prepare/2,
    get_prepared/1
  ]).

%% Internal exports - gen_server callbacks
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3
       ]).

%% Records
-include("mysql.hrl").

-record(state, {}).

%% Macros

-define(TABLE, mysql_statement_versions).

%% External functions

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [self()], []).

start() ->
  gen_server:start({local, ?MODULE}, ?MODULE, [self()], []).

stop() ->
  gen_server:call(?MODULE, stop).

%% This is here for use in tests.
%% Stops the process and deletes the ets table.
stop_and_cleanup() ->
  gen_server:call(?MODULE, stop),
  ets:delete(?TABLE).

prepare(Name, Query) ->
  gen_server:call(?MODULE, {prepare, Name, Query}).

%% @doc Get the prepared statement with the given name.
%%
%%  This function is called from mysql_conn when the connection is
%%  told to execute a prepared statement it has not yet prepared, or
%%  when it is told to execute a statement inside a transaction and
%%  it's not sure that it has the latest version of the statement.
%%
%%  If the latest version of the prepared statement matches the Version
%%  parameter, the return value is {ok, latest}. This saves the cost
%%  of sending the query when the connection already has the latest version.
%%
%% @spec get_prepared(Name::atom(), Version::integer()) ->
%%   {ok, latest} | {ok, Statement::binary()} | {error, Err}
get_prepared(Name) ->
  case ets:lookup(?TABLE, Name) of
    [] ->
      {error, {undefined, Name}};
    [{Name, StatementInfo}] ->
      {ok, StatementInfo}
  end.

%% gen_server callbacks

init([TableHeir]) ->
  EtsOptions = [set, named_table, public, {heir, TableHeir, undefined}, {read_concurrency, true}],
  ets:new(?TABLE, EtsOptions),
  {ok, #state{}}.

handle_call({prepare, Name, Statement}, _, State) ->
  Reply = case ets:lookup(?TABLE, Name) of
    [{Name, Statement}] ->
      ok;
    [{Name, _Other}] ->
      {error, statement_exists};
    [] ->
      ets:insert(?TABLE, {Name, Statement}),
      ok
  end,
  {reply, Reply, State};

handle_call(stop, _, State) ->
  {stop, normal, stopped, State};

handle_call(_, _, State) ->
  {reply, error, State}.

handle_cast(_, State) ->
  {noreply, State}.

handle_info(_, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_, State) ->
  {ok, State}.
