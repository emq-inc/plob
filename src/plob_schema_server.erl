%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Schema manager
%%% @end
%%% Created : 13 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(plob_schema_server).

-behaviour(gen_server).

-include("plob.hrl").

%% API
-export([start_link/0,
         set_schema/1, set_schema/2,
         get_schema/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(TABLE, plob_schemas).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


set_schema(Schema) ->
    set_schema(Schema#schema.table, munge_schema(Schema)).

set_schema(Name, Schema) ->
    gen_server:call(?SERVER, {set_schema, Name, Schema}).

-spec get_schema(atom()) -> #schema{}.
get_schema(Name) ->
    ets:lookup_element(?TABLE, Name, 2).
             

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    ets:new(?TABLE, [set, protected, named_table]),
    {ok, #state{}}.

%%%===================================================================

handle_call({set_schema, Name, Schema}, _From, State) ->
    ets:insert(?TABLE, {Name, Schema}),
    {reply, ok, State};

handle_call(Request, _From, State) ->
    lager:warning("Unexpected call: ~p", [Request]),
    {reply, ok, State}.

%%%===================================================================

handle_cast(Msg, State) ->
    lager:warning("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

%%%===================================================================

handle_info(Info, State) ->
    lager:warning("Unexpected info: ~p", [Info]),
    {noreply, State}.

%%%===================================================================

terminate(normal, _State) ->
    ok;
terminate(Reason, _State) ->
    lager:debug("Terminating: ~p", [Reason]),
    ok.

%%%===================================================================

code_change(OldVsn, State, Extra) ->
    lager:info("Upgrade from version ~p, extra ~p", [OldVsn, Extra]),
    {ok, State}.



%%%===================================================================
%%% Internal functions
%%%===================================================================

munge_schema(Schema) ->
    Schema#schema{
      fields = [munge_field(F) || F <- Schema#schema.fields ]
     }.

munge_field(F) when is_atom(F) -> #field{ name = F };
munge_field(F) -> F.

