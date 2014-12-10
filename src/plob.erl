%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Simple ORM for PostgreSQL
%%% @end
%%% Created : 11 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(plob).

-include("plob.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
         pkget/2
        ]).


-type where() :: [{fieldname(), erlval()}].

-record(select, {
          schema :: #schema{},
          columns :: [fieldname()],
          where :: where() | undefined
         }).


%%%===================================================================
%%% API
%%%===================================================================

-spec pkget(values(), #schema{}) -> dbquery().
pkget(PKVal, Schema) ->
    compile(#select{
               schema = Schema,
               columns = all_columns(Schema),
               where = pk_where(PKVal, Schema) }).


%%%===================================================================
%%% Encoding and decoding
%%%===================================================================

encode(undefined, Val) -> Val;
encode(Fun, Val) when is_function(Val) -> Fun(Val);
encode(Other, _Val) -> 
    throw({no_such_encoder, Other}).


%%%===================================================================
%%% Internal functions
%%%===================================================================

compile(#select{}=Select) ->
    SQL = list_to_binary(
            [<<"SELECT ">>, sql_col_list(Select#select.columns),
             <<" FROM ">>, atom_to_list(Select#select.schema#schema.table),
             <<" WHERE ">>, compile_where(Select#select.where)
            ]),
    Bindings = bind_where(Select#select.where, Select#select.schema),
    {SQL, Bindings}.

-spec compile_where(where()) -> iodata().
compile_where(Where) ->
    compile_where(Where, 1, []).

compile_where([], _, Bits) ->
    term_join(Bits, <<" AND ">>, []);
compile_where([{Name, _Val}|Rest], Count, Bits) when is_atom(Name) ->
    Bit = [atom_to_list(Name), <<" = $">>, integer_to_binary(Count)],
    compile_where(Rest, Count+1, [Bit|Bits]).


-spec bind_where(where(), #schema{}) -> [dbval()].
bind_where(Where, Schema) ->
    [encode_val(Fieldname, Val, Schema)
     || {Fieldname, Val} <- Where].


-spec all_columns(#schema{}) -> [fieldname()].
all_columns(#schema{fields=Fields}) ->
    lists:flatten([field_columns(F) || F <- Fields]).
    

-spec pk_where(values(), #schema{}) -> where().
pk_where(Val, #schema{pk=PK}) when is_atom(PK) ->
    [{PK, Val}];
pk_where(Vals, #schema{pk=PK}) when is_list(PK) ->
    lists:zip(PK, Vals).

-spec field_columns(#field{}) -> columns().
field_columns(#field{name=Name, columns=undefined}) -> Name;
field_columns(#field{columns=Columns}) -> Columns.

-spec sql_col_list([atom()]) -> iodata().
sql_col_list(Cols) ->
    term_join([atom_to_list(Col) || Col <- Cols], <<", ">>).


-spec encode_val(fieldname(), erlval(), #schema{}) -> dbval().
encode_val(Fieldname, Val, Schema) ->
    Field = get_field(Fieldname, Schema),
    encode(Field#field.encoder, Val).

-spec get_field(fieldname(), #schema{}) -> #field{}.
get_field(Fieldname, #schema{}=Schema) ->
    get_field2(Fieldname, Schema#schema.fields).


get_field2(Fieldname, []) ->
    throw({field_not_found, Fieldname});
get_field2(Fieldname, [#field{name=Fieldname}=Field|_]) ->
    Field;
get_field2(Fieldname, [_|Rest]) ->
    get_field2(Fieldname, Rest).


-spec term_join([any()], any()) -> [any()].
term_join(Parts, Sep) ->
    lists:reverse(term_join(Parts, Sep, [])).

term_join([], _, Bits) ->
    Bits;
term_join([Last], _, Bits) ->
    [Last|Bits];
term_join([Next|Rest], Sep, Bits) ->
    term_join(Rest, Sep, [Sep,Next|Bits]).


%%%===================================================================
%%% EUnit
%%%===================================================================


-define(TEST_SCHEMA_SIMPLE, 
        #schema{
           table = test_table,
           pk = id,
           fields = [#field{ name = id },
                     #field{ name = value, 
                             default = <<"Hello">> }] 
          }).
                        
                     

pkget_test() ->
    {<<"SELECT id, value FROM test_table WHERE id = $1">>, [1]}
        = pkget(1, ?TEST_SCHEMA_SIMPLE).


    

