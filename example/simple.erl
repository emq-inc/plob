%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Super basic plop usage
%%% @end
%%% Created : 11 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(simple).

% Use include_lib from other applications
-include("plob.hrl").

-export([get_one/0]).

test_table_schema() ->
    #schema{
       table = test_table,
       pk = id,
       fields = [#field{ name = id },
                 #field{ name = value, 
                         default = <<"Hello">> }] 
      }.


get_one() ->
    {SQL, Bindings} = plob:pkget(1, test_table_schema()),
    io:format("SQL: ~s~n", [SQL]),
    io:format("Bindings: ~p~n", [Bindings]).

    
                        
