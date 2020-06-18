%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% High-level interface
%%% @end
%%% Created : 13 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(plob).

-include("plob.hrl").
-include("plob_compile.hrl").

-export([
         start/0,

         get/2,
         find/2,
         find/3,
         insert/2,
         
         update/2,
         update/3,
         delete/2,

         decode/2,
         decode_one/2
        ]).

-define(SCHEMA(Name), plob_schema_server:get_schema(Name)).


start() ->
    application:ensure_all_started(plob).


-spec get([erlval()] | rowvals(), #schema{} | atom()) -> #dbquery{}.
get(Vals, Schema) when is_atom(Schema) ->
    get(Vals, ?SCHEMA(Schema));
get(#{}=Vals, Schema) ->
    PKs = get_pk(Schema),
    PKVals = [maps:get(PK, Vals) || PK <- PKs],
    get(PKVals, Schema);
get(PK, Schema) ->
    plob_compile:compile(
      plob_query:lookup(PK, plob_query:get_obj(Schema))).


-spec find(wherespec(), #schema{} | atom()) -> #dbquery{}.
find(Vals, Schema) ->
    find(Vals, Schema, #{}).

-spec find(wherespec(), #schema{} | atom(), map()) -> #dbquery{}.
find(Vals, Schema, Opts) when is_atom(Schema) ->
    find(Vals, ?SCHEMA(Schema), Opts);
find(Vals, Schema, Opts) ->
    Filter = plob_query:filter(Vals, Schema, plob_query:get_obj(Schema)),
    plob_compile:compile(
      Filter#select{ limit = maps:get(limit, Opts, undefined),
                     offset = maps:get(offset, Opts, undefined),
                     order = maps:get(order, Opts, undefined) }).


-spec insert(rowvals(), #schema{} | atom()) -> #dbquery{}.
insert(Vals, Schema) when is_atom(Schema) ->
    insert(Vals, ?SCHEMA(Schema));
insert(Vals, Schema) ->
    plob_compile:compile(plob_query:insert(Vals, Schema)).


-spec update(rowvals(), #schema{} | atom()) -> #dbquery{}.
update(Vals, Schema) when is_atom(Schema) ->
    update(Vals, ?SCHEMA(Schema));
update(Vals, Schema) ->
    PKNames = get_pk(Schema),
    {PKVals, OtherVals} = 
        maps:fold(
          fun(Fieldname, Value, {P, O}) ->
                  case lists:member(Fieldname, PKNames) of
                      true -> {maps:put(Fieldname, Value, P), O};
                      false -> {P, maps:put(Fieldname, Value, O)}
                  end
          end, {#{}, #{}}, Vals),
    case maps:size(PKVals) == length(PKNames) of
        true -> ok;
        false -> throw(missing_pks)
    end,
    case maps:size(OtherVals) of
        0 -> throw(nothing_to_update);
        _ -> ok
    end,
    update(OtherVals, PKVals, Schema).


-spec update(rowvals(), wherespec(), #schema{} | atom()) -> #dbquery{}.
update(Vals, Where, Schema) when is_atom(Schema) ->
    update(Vals, Where, ?SCHEMA(Schema));
update(Vals, Where, Schema) ->
    plob_compile:compile(plob_query:update(Vals, Where, Schema)).


-spec delete(wherespec(), #schema{} | atom()) -> #dbquery{}.
delete(Where, Schema) when is_atom(Schema) ->
    delete(Where, ?SCHEMA(Schema));
delete(Where, Schema) ->
    plob_compile:compile(plob_query:delete(Where, Schema)).

-spec decode(#dbquery{}, #dbresult{}) -> [rowvals()].
decode(Query, Result) ->
    plob_query:decode_all(Query, Result).


-spec decode_one(#dbquery{}, #dbresult{}) -> {ok, rowvals()} | {error, any()}.
decode_one(Query, Result) ->
    plob_query:decode_one(Query, Result).



get_pk(#schema{pk=PK}) when is_atom(PK) -> [PK];
get_pk(#schema{pk=PK}) when is_list(PK) -> PK.
     
