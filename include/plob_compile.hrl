%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Types for building queries
%%% @end
%%% Created : 13 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------

-record(select, {
          fields :: fieldset(),
          where = [] :: fieldset(),
          limit :: integer() | undefined
         }).

-record(insert, {
          fields :: fieldset(),
          return :: fieldset()
         }).

-record(update, {
          fields :: fieldset(),
          where :: fieldset()
         }).

-record(delete, {
          where :: fieldset()
         }).

-type operation() :: #select{} | #insert{} | #update{}.

-record(binding, {
          col :: binary(),
          val :: dbval()
         }).

-type unbound_query() :: [binary() | #binding{} | unbound_query()].
