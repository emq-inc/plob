%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Simple ORM for PostgreSQL
%%% @end
%%% Created : 13 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(plob).

-include("plob.hrl").
-include("plob_compile.hrl").

-export([
         get/2,
         find/2,
         insert/2,
         
         update/2,
         update/3,

         decode/2,
         decode_one/2
        ]).

-spec get([erlval()] | rowvals(), #schema{}) -> #dbquery{}.
get(#{}=Vals, Schema) ->
    PKs = get_pk(Schema),
    PKVals = [maps:get(PK, Vals) || PK <- PKs],
    get(PKVals, Schema);
get(PK, Schema) ->
    plob_compile:compile(
      plob_query:lookup(PK, plob_query:get_obj(Schema))).


-spec find(rowvals(), #schema{}) -> #dbquery{}.
find(Vals, Schema) ->
    plob_compile:compile(
      plob_query:filter(Vals, Schema, plob_query:get_obj(Schema))).


-spec insert(rowvals(), #schema{}) -> #dbquery{}.
insert(Vals, Schema) ->
    plob_compile:compile(plob_query:insert(Vals, Schema)).


-spec update(rowvals(), #schema{}) -> #dbquery{}.
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

-spec update(rowvals(), rowvals(), #schema{}) -> #dbquery{}.
update(Vals, Where, Schema) ->
    plob_compile:compile(plob_query:update(Vals, Where, Schema)).


-spec decode(#dbquery{}, #dbresult{}) -> [rowvals()].
decode(Query, Result) ->
    plob_query:decode_all(Query, Result).


-spec decode_one(#dbquery{}, #dbresult{}) -> {ok, rowvals()} | {error, any()}.
decode_one(Query, Result) ->
    plob_query:deocde_one(Query, Result).



get_pk(#schema{pk=PK}) when is_atom(PK) -> [PK];
get_pk(#schema{pk=PK}) when is_list(PK) -> PK.
     
