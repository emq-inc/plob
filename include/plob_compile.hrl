%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Types for building queries
%%% @end
%%% Created : 13 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------

-record(whereval, {
          conjunction = 'and' :: conjunction(),
          fieldvals = [] :: [fieldvals() | #whereval{}]
        }).

-record(select, { schema :: #schema{},
                  fields :: fieldvals(),
                  where = #whereval{},
                  limit :: integer() | undefined,
                  offset :: integer() | undefined,
                  order :: binary() | undefined
                }).

-record(insert, {
          schema :: #schema{},
          fields :: fieldvals(),
          return :: fieldvals()
         }).

-record(update, {
          schema :: #schema{},
          fields :: fieldvals(),
          where :: #whereval{}
         }).

-record(delete, {
          schema :: #schema{},
          where :: #whereval{}
         }).

-type operation() :: #select{} | #insert{} | #update{}.


-record(binding, {
          col :: binary(),
          op = <<"=">> :: binary(),
          val :: dbval()
         }).

-type unbound_query() :: [binary() | #binding{} | unbound_query()].
