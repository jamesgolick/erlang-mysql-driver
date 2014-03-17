%%%-------------------------------------------------------------------
%%% File    : mysql_conn.erl
%%% Author  : Fredrik Thulin <ft@it.su.se>
%%% Descrip.: MySQL connection handler, handles de-framing of messages
%%%           received by the MySQL receiver process.
%%% Created :  5 Aug 2005 by Fredrik Thulin <ft@it.su.se>
%%% Modified: 11 Jan 2006 by Mickael Remond <mickael.remond@process-one.net>
%%%
%%% Note    : All MySQL code was written by Magnus Ahltorp, originally
%%%           in the file mysql.erl - I just moved it here.
%%%
%%% Modified: 12 Sep 2006 by Yariv Sadan <yarivvv@gmail.com>
%%% Added automatic type conversion between MySQL types and Erlang types
%%% and different logging style.
%%%
%%% Modified: 23 Sep 2006 by Yariv Sadan <yarivvv@gmail.com>
%%% Added transaction handling and prepared statement execution.
%%%
%%% Copyright (c) 2001-2004 Kungliga Tekniska H�gskolan
%%% See the file COPYING
%%%
%%%
%%% This module handles a single connection to a single MySQL server.
%%% You can use it stand-alone, or through the 'mysql' module if you
%%% want to have more than one connection to the server, or
%%% connections to different servers.
%%%
%%% To use it stand-alone, set up the connection with
%%%
%%%   {ok, Pid} = mysql_conn:start(Host, Port, User, Password,
%%%                                Database, LogFun)
%%%
%%%         Host     = string()
%%%         Port     = integer()
%%%         User     = string()
%%%         Password = string()
%%%         Database = string()
%%%         LogFun   = undefined | (gives logging to console)
%%%                    function() of arity 3 (Level, Fmt, Args)
%%%
%%% Note: In stand-alone mode you have to start Erlang crypto application by
%%% yourself with crypto:start()
%%%
%%% and then make MySQL querys with
%%%
%%%   Result = mysql_conn:fetch(Pid, Query, self())
%%%
%%%         Result = {data, MySQLRes}    |
%%%                  {updated, MySQLRes} |
%%%                  {error, MySQLRes}
%%%          Where: MySQLRes = #mysql_result
%%%
%%% Actual data can be extracted from MySQLRes by calling the following API
%%% functions:
%%%     - on data received:
%%%          FieldInfo = mysql:get_result_field_info(MysqlRes)
%%%          AllRows   = mysql:get_result_rows(MysqlRes)
%%%         with FieldInfo = list() of {Table, Field, Length, Name}
%%%          and AllRows = list() of list() representing records
%%%     - on update:
%%%          Affected= mysql:get_result_affected_rows(MysqlRes)
%%%         with Affected = integer()
%%%     - on error:
%%%          Reason    = mysql:get_result_reason(MysqlRes)
%%%         with Reason = string()
%%%-------------------------------------------------------------------

-module(mysql_conn).

-behaviour(gen_server).
-behaviour(poolboy_worker).

-include_lib("mysql.hrl").

% public api
-export([start/2,
	 start_link/2,
	 stop/1,
	 fetch/2,
	 fetch/3,
	 execute/3,
	 execute/4,
	 transaction/2,
	 transaction/3,
	 rollback/1,
	 rollback/2
	]).

% poolboy_worker behaviour
-export([start_link/1]).

% gen_server behaviour
-export([init/1,
	 handle_call/3,
	 handle_cast/2,
	 handle_info/2,
	 terminate/2,
	 code_change/3
       ]).

-record(state, {
	  mysql_version,
	  log_fun,
	  recv_pid,
	  socket,
	  data,

	  %% maps statement names to their versions
	  prepares = sets:new(),

	  %% the id of the connection pool to which this connection belongs
	  pool_id
	 }).

-define(SECURE_CONNECTION, 32768).
-define(MYSQL_QUERY_OP, 3).
-define(DEFAULT_STANDALONE_TIMEOUT, 5000).
-define(MYSQL_4_0, 40). %% Support for MySQL 4.0.x
-define(MYSQL_4_1, 41). %% Support for MySQL 4.1.x et 5.0.x

%% Used by transactions to get the state variable for this connection
%% when bypassing the dispatcher.
-define(STATE_VAR, mysql_connection_state).

-define(Log(LogFun,Level,Msg),
	LogFun(?MODULE, ?LINE,Level,fun()-> {Msg,[]} end)).
-define(Log2(LogFun,Level,Msg,Params),
	LogFun(?MODULE, ?LINE,Level,fun()-> {Msg,Params} end)).
-define(L(Msg), io:format("~p:~b ~p ~n", [?MODULE, ?LINE, Msg])).


%%====================================================================
%% External functions
%%====================================================================

start_link(Args) ->
  ConnectionInfo = #mysql_connection_info{
    host=proplists:get_value(host, Args),
    port=proplists:get_value(port, Args),
    user=proplists:get_value(user, Args),
    password=proplists:get_value(password, Args),
    database=proplists:get_value(database, Args),
    log_fun=fun(_,_,_,_) -> ok end,
    encoding=proplists:get_value(encoding, Args)
  },
  start_link(ConnectionInfo, poolid).

start(ConnectionInfo, PoolId) ->
  gen_server:start(?MODULE, [ConnectionInfo, PoolId], []).

start_link(ConnectionInfo, PoolId) ->
  gen_server:start_link(?MODULE, [ConnectionInfo, PoolId], []).

stop(Pid) ->
  gen_server:call(Pid, stop).

fetch(Pid, Queries) ->
  fetch(Pid, Queries, ?DEFAULT_STANDALONE_TIMEOUT).

fetch(Pid, Queries, Timeout)  ->
  gen_server:call(Pid, {fetch, Queries}, Timeout).

execute(Pid, Name, Params) ->
  execute(Pid, Name, Params, ?DEFAULT_STANDALONE_TIMEOUT).

execute(Pid, Name, Params, Timeout) ->
  gen_server:call(Pid, {execute, Name, Params}, Timeout).

transaction(Pid, Fun) ->
  transaction(Pid, Fun, ?DEFAULT_STANDALONE_TIMEOUT).

transaction(Pid, Fun, Timeout) ->
  ok = gen_server:call(Pid, start_transaction, Timeout),
  case catch Fun() of
    error = Err -> rollback(Pid, Err);
    {error, _} = Err -> rollback(Pid, Err);
    {'EXIT', {noproc, _}} -> {aborted, connection_exited};
    {'EXIT', _} = Err -> rollback(Pid, Err);
    Res ->
      case gen_server:call(Pid, commit_transaction, Timeout) of
	{error, _} = Err -> rollback(Pid, Err);
	_ ->
	  case Res of
	    aborted -> aborted;
	    {atomic, _} -> Res;
	    _ -> {atomic, Res}
	  end
      end
  end.

rollback(Pid) ->
  rollback(Pid, undefined).

rollback(Pid, Error) ->
  gen_server:call(Pid, {rollback_transaction, Error}).

%%--------------------------------------------------------------------
%% Function: do_recv(LogFun, RecvPid, SeqNum)
%%           LogFun  = undefined | function() with arity 3
%%           RecvPid = pid(), mysql_recv process
%%           SeqNum  = undefined | integer()
%% Descrip.: Wait for a frame decoded and sent to us by RecvPid.
%%           Either wait for a specific frame if SeqNum is an integer,
%%           or just any frame if SeqNum is undefined.
%% Returns : {ok, Packet, Num} |
%%           {error, Reason}
%%           Reason = term()
%%
%% Note    : Only to be used externally by the 'mysql_auth' module.
%%--------------------------------------------------------------------
do_recv(LogFun, RecvPid, SeqNum)  when is_function(LogFun);
				       LogFun == undefined,
				       SeqNum == undefined ->
    receive
        {mysql_recv, RecvPid, data, Packet, Num} ->
	    {ok, Packet, Num};
	{mysql_recv, RecvPid, closed, _E} ->
	    {error, io_lib:format("mysql_recv: socket was closed ~p", [_E])}
    end;
do_recv(LogFun, RecvPid, SeqNum) when is_function(LogFun);
				      LogFun == undefined,
				      is_integer(SeqNum) ->
    ResponseNum = SeqNum + 1,
    receive
        {mysql_recv, RecvPid, data, Packet, ResponseNum} ->
	    {ok, Packet, ResponseNum};
	{mysql_recv, RecvPid, closed, _E} ->
	    {error, io_lib:format("mysql_recv: socket was closed ~p", [_E])}
    end.

init([ConnectionInfo, PoolId]) ->
  #mysql_connection_info{host=Host,
			 port=Port,
			 user=User,
			 password=Password,
			 database=Database,
			 log_fun=LogFun,
			 encoding=Encoding} = ConnectionInfo,
  case mysql_recv:start_link(Host, Port, LogFun, self()) of
    {ok, RecvPid, Sock} ->
      case mysql_init(Sock, RecvPid, User, Password, LogFun) of
	{ok, Version} ->
	  Db = iolist_to_binary(Database),
	  case do_query(Sock, RecvPid, LogFun,
	      <<"use ", Db/binary>>,
	      Version) of
	    {error, MySQLRes} ->
	      ?Log2(LogFun, error,
		"mysql_conn: Failed changing to database "
		"~p : ~p",
		[Database,
		  get_result_reason(MySQLRes)]),
	      {error, failed_changing_database};
	    {_ResultType, _MySQLRes} ->
	      case Encoding of
		undefined -> undefined;
		_ ->
		  EncodingBinary = list_to_binary(atom_to_list(Encoding)),
		  do_query(Sock, RecvPid, LogFun,
		    <<"set names '", EncodingBinary/binary, "'">>,
		    Version)
	      end,
	      State = #state{mysql_version=Version,
		recv_pid = RecvPid,
		socket   = Sock,
		log_fun  = LogFun,
		pool_id  = PoolId,
		data     = <<>>
	      },
	      {ok, State}
	  end;
	{error, _Reason} ->
	  {error, login_failed}
      end;
    E ->
      ?Log2(LogFun, error,
	"failed connecting to ~p:~p : ~p",
	[Host, Port, E]),
      {error, connect_failed}
  end.

handle_call({fetch, Queries}, _, State) ->
  Reply = do_queries(State, Queries),
  {reply, Reply, State};

handle_call({execute, Name, Params}, _, State) ->
  {ok, Statement} = mysql_statement:get_prepared(Name),
  Reply = prepare_and_exec(State, Name, Statement, Params),
  Prepares = sets:add_element(Name, State#state.prepares),
  {reply, Reply, State#state{prepares=Prepares}};

handle_call(start_transaction, _, State) ->
  {reply, start_transaction(State), State};

handle_call({rollback_transaction, Error}, _, State) ->
  {reply, rollback_transaction(Error, State), State};

handle_call(commit_transaction, _, State) ->
  {reply, commit_transaction(State), State};

handle_call(stop, _, State) ->
  {stop, normal, stopped, State};

handle_call(_, _, State) ->
  {reply, ok, State}.

handle_cast(_, State) ->
  {noreply, State}.

handle_info(_, State) ->
  {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_, State) ->
  {ok, State}.

do_query(State, Query) ->
    do_query(State#state.socket,
	       State#state.recv_pid,
	       State#state.log_fun,
	       Query,
	       State#state.mysql_version
	      ).

do_query(Sock, RecvPid, LogFun, Query, Version) ->
    Query1 = iolist_to_binary(Query),
    ?Log2(LogFun, debug, "fetch ~p (id ~p)", [Query1,RecvPid]),
    Packet =  <<?MYSQL_QUERY_OP, Query1/binary>>,
    case do_send(Sock, Packet, 0, LogFun) of
	ok ->
	    get_query_response(LogFun,RecvPid,
				    Version);
	{error, Reason} ->
	    Msg = io_lib:format("Failed sending data "
				"on socket : ~p",
				[Reason]),
	    {error, Msg}
    end.

do_queries(State, Queries) when not is_list(Queries) ->
    do_query(State, Queries);
do_queries(State, Queries) ->
    do_queries(State#state.socket,
	       State#state.recv_pid,
	       State#state.log_fun,
	       Queries,
	       State#state.mysql_version
	      ).

%% Execute a list of queries, returning the response for the last query.
%% If a query returns an error before the last query is executed, the
%% loop is aborted and the error is returned. 
do_queries(Sock, RecvPid, LogFun, Queries, Version) ->
    catch
	lists:foldl(
	  fun(Query, _LastResponse) ->
		  case do_query(Sock, RecvPid, LogFun, Query, Version) of
		      {error, _} = Err -> throw(Err);
		      Res -> Res
		  end
	  end, ok, Queries).

start_transaction(State) ->
  case do_query(State, <<"BEGIN">>) of
    {error, _} = Err ->	
      {aborted, Err};
    _ ->
      ok
  end.

rollback_transaction(undefined, State) ->
  {updated, #mysql_result{}} = do_query(State, <<"ROLLBACK">>),
  aborted;
rollback_transaction(Err, State) ->
  Res = do_query(State, <<"ROLLBACK">>),
  {aborted, {Err, {rollback_result, Res}}}.

commit_transaction(State) ->
  Res = do_query(State, <<"COMMIT">>),
  {committed, Res}.

prepare_and_exec(State, Name, Stmt, Params) ->
    NameBin = atom_to_binary(Name),
    StmtBin = <<"PREPARE ", NameBin/binary, " FROM '",
		Stmt/binary, "'">>,
    case do_query(State, StmtBin) of
	{updated, _} ->
	    do_execute1(State, Name, Params);
	{error, _} = Err ->
	    Err;
	Other ->
	    {error, {unexpected_result, Other}}
    end.

do_execute1(State, Name, Params) ->
    Stmts = make_statements_for_execute(Name, Params),
    do_queries(State, Stmts).

make_statements_for_execute(Name, []) ->
    NameBin = atom_to_binary(Name),
    [<<"EXECUTE ", NameBin/binary>>];
make_statements_for_execute(Name, Params) ->
    NumParams = length(Params),
    ParamNums = lists:seq(1, NumParams),

    NameBin = atom_to_binary(Name),
    
    ParamNames =
	lists:foldl(
	  fun(Num, Acc) ->
		  ParamName = [$@ | integer_to_list(Num)],
		  if Num == 1 ->
			  ParamName ++ Acc;
		     true ->
			  [$, | ParamName] ++ Acc
		  end
	  end, [], lists:reverse(ParamNums)),
    ParamNamesBin = list_to_binary(ParamNames),

    ExecStmt = <<"EXECUTE ", NameBin/binary, " USING ",
		ParamNamesBin/binary>>,

    ParamVals = lists:zip(ParamNums, Params),
    Stmts = lists:foldl(
	      fun({Num, Val}, Acc) ->
		      NumBin = encode(Num, true),
		      ValBin = encode(Val, true),
		      [<<"SET @", NumBin/binary, "=", ValBin/binary>> | Acc]
	       end, [ExecStmt], lists:reverse(ParamVals)),
    Stmts.

atom_to_binary(Val) ->
    <<_:4/binary, Bin/binary>> = term_to_binary(Val),
    Bin.

%%--------------------------------------------------------------------
%% Function: mysql_init(Sock, RecvPid, User, Password, LogFun)
%%           Sock     = term(), gen_tcp socket
%%           RecvPid  = pid(), mysql_recv process
%%           User     = string()
%%           Password = string()
%%           LogFun   = undefined | function() with arity 3
%% Descrip.: Try to authenticate on our new socket.
%% Returns : ok | {error, Reason}
%%           Reason = string()
%%--------------------------------------------------------------------
mysql_init(Sock, RecvPid, User, Password, LogFun) ->
    case do_recv(LogFun, RecvPid, undefined) of
	{ok, Packet, InitSeqNum} ->
	    {Version, Salt1, Salt2, Caps} = greeting(Packet, LogFun),
	    AuthRes =
		case Caps band ?SECURE_CONNECTION of
		    ?SECURE_CONNECTION ->
			mysql_auth:do_new_auth(
			  Sock, RecvPid, InitSeqNum + 1,
			  User, Password, Salt1, Salt2, LogFun);
		    _ ->
			mysql_auth:do_old_auth(
			  Sock, RecvPid, InitSeqNum + 1, User, Password,
			  Salt1, LogFun)
		end,
	    case AuthRes of
		{ok, <<0:8, _Rest/binary>>, _RecvNum} ->
		    {ok,Version};
		{ok, <<255:8, Rest/binary>>, _RecvNum} ->
		    {Code, ErrData} = get_error_data(Rest, Version),
		    ?Log2(LogFun, error, "init error ~p: ~p",
			 [Code, ErrData]),
		    {error, ErrData};
		{ok, RecvPacket, _RecvNum} ->
		    ?Log2(LogFun, error,
			  "init unknown error ~p",
			  [binary_to_list(RecvPacket)]),
		    {error, binary_to_list(RecvPacket)};
		{error, Reason} ->
		    ?Log2(LogFun, error,
			  "init failed receiving data : ~p", [Reason]),
		    {error, Reason}
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

%% part of mysql_init/4
greeting(Packet, LogFun) ->
    <<Protocol:8, Rest/binary>> = Packet,
    {Version, Rest2} = asciz(Rest),
    <<_TreadID:32/little, Rest3/binary>> = Rest2,
    {Salt, Rest4} = asciz(Rest3),
    <<Caps:16/little, Rest5/binary>> = Rest4,
    <<ServerChar:16/binary-unit:8, Rest6/binary>> = Rest5,
    {Salt2, _Rest7} = asciz(Rest6),
    ?Log2(LogFun, debug,
	  "greeting version ~p (protocol ~p) salt ~p caps ~p serverchar ~p"
	  "salt2 ~p",
	  [Version, Protocol, Salt, Caps, ServerChar, Salt2]),
    {normalize_version(Version, LogFun), Salt, Salt2, Caps}.

%% part of greeting/2
asciz(Data) when is_binary(Data) ->
    asciz_binary(Data, []);
asciz(Data) when is_list(Data) ->
    {String, [0 | Rest]} = lists:splitwith(fun (C) ->
						   C /= 0
					   end, Data),
    {String, Rest}.

%%--------------------------------------------------------------------
%% Function: get_query_response(LogFun, RecvPid)
%%           LogFun  = undefined | function() with arity 3
%%           RecvPid = pid(), mysql_recv process
%%           Version = integer(), Representing MySQL version used
%% Descrip.: Wait for frames until we have a complete query response.
%% Returns :   {data, #mysql_result}
%%             {updated, #mysql_result}
%%             {error, #mysql_result}
%%           FieldInfo    = list() of term()
%%           Rows         = list() of [string()]
%%           AffectedRows = int()
%%           Reason       = term()
%%--------------------------------------------------------------------
get_query_response(LogFun, RecvPid, Version) ->
    case do_recv(LogFun, RecvPid, undefined) of
	{ok, Packet, _} ->
	    {Fieldcount, Rest} = get_lcb(Packet),
	    case Fieldcount of
		0 ->
		    %% No Tabular data
		    {AffectedRows, Rest2} = get_lcb(Rest),
		    {InsertId, _} = get_lcb(Rest2),
		    {updated, #mysql_result{affectedrows=AffectedRows, insertid=InsertId}};
		255 ->
		    case get_error_data(Rest, Version) of
			{Code, {SqlState, Message}} ->	 
			    % MYSQL_4_1 error data
			    {error, #mysql_result{error=Message, 
						  errcode=Code,
						  errsqlstate=SqlState}};
			{Code, Message} -> 
	   		    % MYSQL_4_0 error data
			    {error, #mysql_result{error=Message,
						  errcode=Code}}
		    end;
		_ ->
		    %% Tabular data received
		    case get_fields(LogFun, RecvPid, [], Version) of
			{ok, Fields} ->
			    case get_rows(Fields, LogFun, RecvPid, [], Version) of
				{ok, Rows} ->
				    {data, #mysql_result{fieldinfo=Fields,
							 rows=Rows}};
				{error, {Code, {SqlState, Message}}} ->	 
				    % MYSQL_4_1 error data
				    {error, #mysql_result{error=Message, 
							  errcode=Code,
							  errsqlstate=SqlState}};
				{error, {Code, Message}} -> 
				    % MYSQL_4_0 error data
				    {error, #mysql_result{error=Message,
							  errcode=Code}}
			    end;
			{error, Reason} ->
			    {error, #mysql_result{error=Reason}}
		    end
	    end;
	{error, Reason} ->
	    {error, #mysql_result{error=Reason}}
    end.

%%--------------------------------------------------------------------
%% Function: get_fields(LogFun, RecvPid, [], Version)
%%           LogFun  = undefined | function() with arity 3
%%           RecvPid = pid(), mysql_recv process
%%           Version = integer(), Representing MySQL version used
%% Descrip.: Received and decode field information.
%% Returns : {ok, FieldInfo} |
%%           {error, Reason}
%%           FieldInfo = list() of term()
%%           Reason    = term()
%%--------------------------------------------------------------------
%% Support for MySQL 4.0.x:
get_fields(LogFun, RecvPid, Res, ?MYSQL_4_0) ->
    case do_recv(LogFun, RecvPid, undefined) of
	{ok, Packet, _Num} ->
	    case Packet of
		<<254:8>> ->
		    {ok, lists:reverse(Res)};
		<<254:8, Rest/binary>> when size(Rest) < 8 ->
		    {ok, lists:reverse(Res)};
		_ ->
		    {Table, Rest} = get_with_length(Packet),
		    {Field, Rest2} = get_with_length(Rest),
		    {LengthB, Rest3} = get_with_length(Rest2),
		    LengthL = size(LengthB) * 8,
		    <<Length:LengthL/little>> = LengthB,
		    {Type, Rest4} = get_with_length(Rest3),
		    {_Flags, _Rest5} = get_with_length(Rest4),
		    This = {Table,
			    Field,
			    Length,
			    %% TODO: Check on MySQL 4.0 if types are specified
			    %%       using the same 4.1 formalism and could 
			    %%       be expanded to atoms:
			    Type},
		    get_fields(LogFun, RecvPid, [This | Res], ?MYSQL_4_0)
	    end;
	{error, Reason} ->
	    {error, Reason}
    end;
%% Support for MySQL 4.1.x and 5.x:
get_fields(LogFun, RecvPid, Res, ?MYSQL_4_1) ->
    case do_recv(LogFun, RecvPid, undefined) of
	{ok, Packet, _Num} ->
	    case Packet of
		<<254:8>> ->
		    {ok, lists:reverse(Res)};
		<<254:8, Rest/binary>> when size(Rest) < 8 ->
		    {ok, lists:reverse(Res)};
		_ ->
		    {_Catalog, Rest} = get_with_length(Packet),
		    {_Database, Rest2} = get_with_length(Rest),
		    {Table, Rest3} = get_with_length(Rest2),
		    %% OrgTable is the real table name if Table is an alias
		    {_OrgTable, Rest4} = get_with_length(Rest3),
		    {Field, Rest5} = get_with_length(Rest4),
		    %% OrgField is the real field name if Field is an alias
		    {_OrgField, Rest6} = get_with_length(Rest5),

		    <<_Metadata:8/little, _Charset:16/little,
		     Length:32/little, Type:8/little,
		     _Flags:16/little, _Decimals:8/little,
		     _Rest7/binary>> = Rest6,
		    
		    This = {Table,
			    Field,
			    Length,
			    get_field_datatype(Type)},
		    get_fields(LogFun, RecvPid, [This | Res], ?MYSQL_4_1)
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Function: get_rows(N, LogFun, RecvPid, [], Version)
%%           N       = integer(), number of rows to get
%%           LogFun  = undefined | function() with arity 3
%%           RecvPid = pid(), mysql_recv process
%%           Version = integer(), Representing MySQL version used
%% Descrip.: Receive and decode a number of rows.
%% Returns : {ok, Rows} |
%%           {error, Reason}
%%           Rows = list() of [string()]
%%--------------------------------------------------------------------
get_rows(Fields, LogFun, RecvPid, Res, Version) ->
    case do_recv(LogFun, RecvPid, undefined) of
	{ok, Packet, _Num} ->
	    case Packet of
		<<254:8, Rest/binary>> when size(Rest) < 8 ->
		    {ok, lists:reverse(Res)};
		<<255:8, Rest/binary>> ->
		    {Code, ErrData} = get_error_data(Rest, Version),		    
		    {error, {Code, ErrData}};
		_ ->
		    {ok, This} = get_row(Fields, Packet, []),
		    get_rows(Fields, LogFun, RecvPid, [This | Res], Version)
	    end;
	{error, Reason} ->
	    {error, Reason}
    end.

%% part of get_rows/4
get_row([], _Data, Res) ->
    {ok, lists:reverse(Res)};
get_row([Field | OtherFields], Data, Res) ->
    {Col, Rest} = get_with_length(Data),
    This = case Col of
	       null ->
		   undefined;
	       _ ->
		   convert_type(Col, element(4, Field))
	   end,
    get_row(OtherFields, Rest, [This | Res]).

get_with_length(Bin) when is_binary(Bin) ->
    {Length, Rest} = get_lcb(Bin),
    case get_lcb(Bin) of 
    	 {null, Rest} -> {null, Rest};
    	 _ -> split_binary(Rest, Length)
    end.



get_lcb(<<251:8, Rest/binary>>) ->
    {null, Rest};
get_lcb(<<252:8, Value:16/little, Rest/binary>>) ->
    {Value, Rest};
get_lcb(<<253:8, Value:24/little, Rest/binary>>) ->
    {Value, Rest};
get_lcb(<<254:8, Value:32/little, Rest/binary>>) ->
    {Value, Rest};
get_lcb(<<Value:8, Rest/binary>>) when Value < 251 ->
    {Value, Rest};
get_lcb(<<255:8, Rest/binary>>) ->
    {255, Rest}.

%%--------------------------------------------------------------------
%% Function: do_send(Sock, Packet, SeqNum, LogFun)
%%           Sock   = term(), gen_tcp socket
%%           Packet = binary()
%%           SeqNum = integer(), packet sequence number
%%           LogFun = undefined | function() with arity 3
%% Descrip.: Send a packet to the MySQL server.
%% Returns : result of gen_tcp:send/2
%%--------------------------------------------------------------------
do_send(Sock, Packet, SeqNum, _LogFun) when is_binary(Packet), is_integer(SeqNum) ->
    Data = <<(size(Packet)):24/little, SeqNum:8, Packet/binary>>,
    gen_tcp:send(Sock, Data).

%%--------------------------------------------------------------------
%% Function: normalize_version(Version, LogFun)
%%           Version  = string()
%%           LogFun   = undefined | function() with arity 3
%% Descrip.: Return a flag corresponding to the MySQL version used.
%%           The protocol used depends on this flag.
%% Returns : Version = string()
%%--------------------------------------------------------------------
normalize_version([$4,$.,$0|_T], LogFun) ->
    ?Log(LogFun, debug, "switching to MySQL 4.0.x protocol."),
    ?MYSQL_4_0;
normalize_version([$4,$.,$1|_T], _LogFun) ->
    ?MYSQL_4_1;
normalize_version([$5|_T], _LogFun) ->
    %% MySQL version 5.x protocol is compliant with MySQL 4.1.x:
    ?MYSQL_4_1; 
normalize_version(_Other, LogFun) ->
    ?Log(LogFun, error, "MySQL version not supported: MySQL Erlang module "
	 "might not work correctly."),
    %% Error, but trying the oldest protocol anyway:
    ?MYSQL_4_0.

%%--------------------------------------------------------------------
% Function: get_field_datatype(DataType)
%%           DataType = integer(), MySQL datatype
%% Descrip.: Return MySQL field datatype as description string
%% Returns : String, MySQL datatype
%%--------------------------------------------------------------------
get_field_datatype(0) ->   'DECIMAL';
get_field_datatype(1) ->   'TINY';
get_field_datatype(2) ->   'SHORT';
get_field_datatype(3) ->   'LONG';
get_field_datatype(4) ->   'FLOAT';
get_field_datatype(5) ->   'DOUBLE';
get_field_datatype(6) ->   'NULL';
get_field_datatype(7) ->   'TIMESTAMP';
get_field_datatype(8) ->   'LONGLONG';
get_field_datatype(9) ->   'INT24';
get_field_datatype(10) ->  'DATE';
get_field_datatype(11) ->  'TIME';
get_field_datatype(12) ->  'DATETIME';
get_field_datatype(13) ->  'YEAR';
get_field_datatype(14) ->  'NEWDATE';
get_field_datatype(246) -> 'NEWDECIMAL';
get_field_datatype(247) -> 'ENUM';
get_field_datatype(248) -> 'SET';
get_field_datatype(249) -> 'TINYBLOB';
get_field_datatype(250) -> 'MEDIUM_BLOG';
get_field_datatype(251) -> 'LONG_BLOG';
get_field_datatype(252) -> 'BLOB';
get_field_datatype(253) -> 'VAR_STRING';
get_field_datatype(254) -> 'STRING';
get_field_datatype(255) -> 'GEOMETRY'.

convert_type(Val, ColType) ->
    case ColType of
	T when T == 'TINY';
	       T == 'SHORT';
	       T == 'LONG';
	       T == 'LONGLONG';
	       T == 'INT24';
	       T == 'YEAR' ->
	    list_to_integer(binary_to_list(Val));
	T when T == 'TIMESTAMP';
	       T == 'DATETIME' ->
	    {ok, [Year, Month, Day, Hour, Minute, Second], _Leftovers} =
		io_lib:fread("~d-~d-~d ~d:~d:~d", binary_to_list(Val)),
	    {datetime, {{Year, Month, Day}, {Hour, Minute, Second}}};
	'TIME' ->
	    {ok, [Hour, Minute, Second], _Leftovers} =
		io_lib:fread("~d:~d:~d", binary_to_list(Val)),
	    {time, {Hour, Minute, Second}};
	'DATE' ->
	    {ok, [Year, Month, Day], _Leftovers} =
		io_lib:fread("~d-~d-~d", binary_to_list(Val)),
	    {date, {Year, Month, Day}};
	T when T == 'DECIMAL';
	       T == 'NEWDECIMAL';
	       T == 'FLOAT';
	       T == 'DOUBLE' ->
	    {ok, [Num], _Leftovers} =
		case io_lib:fread("~f", binary_to_list(Val)) of
		    {error, _} ->
			io_lib:fread("~d", binary_to_list(Val));
		    Res ->
			Res
		end,
	    Num;
	_Other ->
	    Val
    end.
	    
get_error_data(ErrPacket, ?MYSQL_4_0) ->
    <<Code:16/little, Message/binary>> = ErrPacket,
    {Code, binary_to_list(Message)};
get_error_data(ErrPacket, ?MYSQL_4_1) ->
    <<Code:16/little, _M:8, SqlState:5/binary, Message/binary>> = ErrPacket,
    {Code, {binary_to_list(SqlState), binary_to_list(Message)}}.

%% @doc Encode a value so that it can be included safely in a MySQL query.
%%
%% @spec encode(Val::term(), AsBinary::bool()) ->
%%   string() | binary() | {error, Error}
encode(Val, false) when Val == undefined; Val == null ->
    "null";
encode(Val, true) when Val == undefined; Val == null ->
    <<"null">>;
encode(Val, false) when is_binary(Val) ->
    binary_to_list(quote(Val));
encode(Val, true) when is_binary(Val) ->
    quote(Val);
encode(Val, true) ->
    list_to_binary(encode(Val,false));
encode(Val, false) when is_atom(Val) ->
    quote(atom_to_list(Val));
encode(Val, false) when is_list(Val) ->
    quote(Val);
encode(Val, false) when is_integer(Val) ->
    integer_to_list(Val);
encode(Val, false) when is_float(Val) ->
    [Res] = io_lib:format("~w", [Val]),
    Res;
encode({datetime, Val}, AsBinary) ->
    encode(Val, AsBinary);
encode({{Year, Month, Day}, {Hour, Minute, Second}}, false) ->
    Res = two_digits([Year, Month, Day, Hour, Minute, Second]),
    lists:flatten(Res);
encode({TimeType, Val}, AsBinary)
  when TimeType == 'date';
       TimeType == 'time' ->
    encode(Val, AsBinary);
encode({Time1, Time2, Time3}, false) ->
    Res = two_digits([Time1, Time2, Time3]),
    lists:flatten(Res);
encode(Val, _AsBinary) ->
    {error, {unrecognized_value, Val}}.

%% @doc Extract the error Reason from MySQL Result on error
%%
%% @spec get_result_reason(MySQLRes::mysql_result()) ->
%%    Reason::string()
get_result_reason(#mysql_result{error=Reason}) ->
    Reason.

%% @doc Find the first zero-byte in Data and add everything before it
%%   to Acc, as a string.
%%
%% @spec asciz_binary(Data::binary(), Acc::list()) ->
%%   {NewList::list(), Rest::binary()}
asciz_binary(<<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
asciz_binary(<<0:8, Rest/binary>>, Acc) ->
    {lists:reverse(Acc), Rest};
asciz_binary(<<C:8, Rest/binary>>, Acc) ->
    asciz_binary(Rest, [C | Acc]).

%%  Quote a string or binary value so that it can be included safely in a
%%  MySQL query.
quote(String) when is_list(String) ->
    [39 | lists:reverse([39 | quote(String, [])])];	%% 39 is $'
quote(Bin) when is_binary(Bin) ->
    list_to_binary(quote(binary_to_list(Bin))).

quote([], Acc) ->
    Acc;
quote([0 | Rest], Acc) ->
    quote(Rest, [$0, $\\ | Acc]);
quote([10 | Rest], Acc) ->
    quote(Rest, [$n, $\\ | Acc]);
quote([13 | Rest], Acc) ->
    quote(Rest, [$r, $\\ | Acc]);
quote([$\\ | Rest], Acc) ->
    quote(Rest, [$\\ , $\\ | Acc]);
quote([39 | Rest], Acc) ->		%% 39 is $'
    quote(Rest, [39, $\\ | Acc]);	%% 39 is $'
quote([34 | Rest], Acc) ->		%% 34 is $"
    quote(Rest, [34, $\\ | Acc]);	%% 34 is $"
quote([26 | Rest], Acc) ->
    quote(Rest, [$Z, $\\ | Acc]);
quote([C | Rest], Acc) ->
    quote(Rest, [C | Acc]).

two_digits(Nums) when is_list(Nums) ->
    [two_digits(Num) || Num <- Nums];
two_digits(Num) ->
    [Str] = io_lib:format("~b", [Num]),
    case length(Str) of
	1 -> [$0 | Str];
	_ -> Str
    end.


