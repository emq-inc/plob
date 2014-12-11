%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Epgsql result handler
%%% @end
%%% Created : 11 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(plob_epgsql).

-include("plob.hrl").

-export([get_tuples/1]).

-spec get_tuples(any()) -> {ok, [[dbval()]]} | {error, any()}.
get_tuples({ok, _Cols, Tuples}) when is_list(Tuples) ->
    {ok, to_lists(Tuples)};
get_tuples({ok, Tuples}) when is_list(Tuples) ->
    {ok, to_lists(Tuples)};
get_tuples({error, Error}) ->
    {error, Error}.


to_lists(Tuples) ->
    lists:map(fun erlang:tuple_to_list/1, Tuples).
