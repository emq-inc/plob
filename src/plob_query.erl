%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Low-level query builder and decoder
%%% @end
%%% Created : 11 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(plob_query).

-include("plob.hrl").
-include("plob_compile.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([
         get_obj/1,
         lookup/2,
         filter/3,

         insert/2,
         update/3,
         delete/2,

         decode_one/2,
         decode_all/2
        ]).

%%%===================================================================
%%% Select builders
%%%===================================================================

-spec get_obj(#schema{}) -> #select{}.
get_obj(Schema) ->
    #select{ schema = Schema,
             fields = select_all_fields(Schema) }.

-spec lookup(values(), #select{}) -> #select{}.
lookup(PKVal, #select{ schema = Schema }=Select) ->
    Select#select{ schema = Schema,
                   where = pk_where(PKVal, Schema) }.

-spec filter(wherespec(), #schema{}, #select{}) -> #select{}.
filter(WhereSpec, Schema, Select) ->
    case lists:any(fun({S, _}) ->
                           S#schema.table =:= Schema#schema.table
                   end,
                   Select#select.fields) of
        true -> ok;
        false -> throw({schema_not_in_wherespec,
                        Schema#schema.table,
                        [S#schema.table ||
                            {S, _} <- Select#select.fields]})
    end,
    Where = spec_to_wherevals(WhereSpec, Schema),
    Select#select{ schema = Schema, where = Where }.


%%%===================================================================
%%% Query builders
%%%===================================================================

-spec insert(rowvals(), #schema{}) -> #insert{}.
insert(Map, Schema) ->
    #insert{ schema = Schema,
             fields = map_to_fieldvals(Map, Schema),
             return = select_pk_fields(Schema) }.


-spec update(rowvals(), rowvals(), #schema{}) -> #update{}.
update(Vals, WhereSpec, Schema) ->
    #update{ schema = Schema,
             fields = map_to_fieldvals(Vals, Schema),
             where = spec_to_wherevals(WhereSpec, Schema) }.


-spec delete(rowvals(), #schema{}) -> #delete{}.
delete(WhereSpec, Schema) ->
    #delete{ schema = Schema,
             where = spec_to_wherevals(WhereSpec, Schema) }.


%%%===================================================================
%%% Row API
%%%===================================================================

-spec decode_one(#dbquery{}, #dbresult{}) -> {ok, rowvals()} | {error, any()}.
decode_one(Query, #dbresult{ raw = Raw, module = Module }) ->
    case Module:get_tuples(Raw) of
        {ok, [List]} ->
            {ok, list_to_row(List, Query)};
        {ok, []} ->
            {error, not_found};
        {ok, Other} when is_list(Other) ->
            {error, multiple_results};
        {error, Error} ->
            {error, Error}
    end.

-spec decode_all(#dbquery{}, #dbresult{}) -> [rowvals()].
decode_all(Query, #dbresult{ raw = Raw, module = Module }) ->
    {ok, Lists} = Module:get_tuples(Raw),
    [list_to_row(List, Query) || List <- Lists].

%%%===================================================================
%%% Internal row functions
%%%===================================================================

-spec list_to_row([dbval()], #dbquery{}) -> rowvals().
% TODO: Join support
list_to_row(List, #dbquery{ fields=Fields }) ->
    collect_field_values(Fields, List, #{}).

-spec collect_field_values([fieldval()], [dbval()], rowvals()) -> rowvals().
collect_field_values([], [], Result) ->
    Result;
collect_field_values([], MoreCols, _) ->
    throw({too_many_columns, MoreCols});
collect_field_values(Fields, [], _) ->
    throw({not_enough_columns, Fields});
collect_field_values([{_Schema, {Field, _}}|Rest], Cols, Result) ->
    {DBVal, NewCols} = collect_field_cols(Field, Cols),
    Value = plob_codec:decode(Field#field.codec, DBVal),
    collect_field_values(Rest, NewCols,
                         maps:put(Field#field.name, Value, Result)).


collect_field_cols(#field{columns=undefined}, [Col|Rest]) ->
    {Col, Rest};
collect_field_cols(#field{columns=Column}, [Col|Rest]) when is_atom(Column) ->
    {Col, Rest};
collect_field_cols(#field{columns=Columns}, Cols) ->
    Count = length(Columns),
    case Count of
        1 -> {hd(Cols), tl(Cols)};
        X when X > length(Cols) ->
            throw({not_enough_columns, Columns, Cols});
        _ -> lists:split(Count, Cols)
    end.


%%%===================================================================
%%% Internal SQL functions
%%%===================================================================

-spec pk_where(values(), #schema{}) -> fieldvals().
pk_where(Vals, #schema{}=Schema) ->
    spec_to_wherevals(pk_fieldvals(Vals, Schema), Schema).

-spec pk_fieldvals(values(), #schema{}) -> [fieldval()].
pk_fieldvals(Val, #schema{pk=PK}) when is_atom(PK) ->
    #{ PK => Val };
pk_fieldvals(Vals, #schema{pk=PK}) when is_list(PK) ->
    maps:from_list(lists:zip(PK, Vals)).


-spec map_to_fieldvals(#{}, #schema{}) -> fieldvals().
map_to_fieldvals(Map, Schema) ->
    [{Schema, {get_field(K, Schema), V}}
     || {K, V} <- maps:to_list(Map),
        V =/= undefined].


-spec spec_to_wherevals(wherespec(), #schema{}) -> #whereval{}.
spec_to_wherevals(RowVals, Schema) when is_map(RowVals) ->
    spec_to_wherevals({'and', RowVals}, Schema);
spec_to_wherevals({Conjugation, RowVals}, Schema) when is_map(RowVals) ->
    #whereval{ conjugation = Conjugation,
               fieldvals = map_to_fieldvals(RowVals, Schema)};
spec_to_wherevals({Conjugation, Nested}, Schema) when is_list(Nested) ->
    #whereval{ conjugation = Conjugation,
               fieldvals = [spec_to_wherevals(WhereSpec, Schema)
                            || WhereSpec <- Nested] }.


-spec select_all_fields(#schema{}) -> fieldvals().
select_all_fields(Schema) ->
    [{Schema, {F, undefined}} || F <- Schema#schema.fields].


-spec select_pk_fields(#schema{}) -> fieldvals().
select_pk_fields(#schema{pk=undefined}=Schema) ->
    select_all_fields(Schema);
select_pk_fields(#schema{pk=PK}=Schema) when is_atom(PK) ->
    [{Schema, {get_field(PK, Schema), undefined}}];
select_pk_fields(#schema{pk=PK}=Schema) when is_list(PK) ->
    [{Schema, {get_field(F, Schema), undefined}}
     || F <- PK].


-spec get_field(fieldname(), #schema{}) -> #field{}.
get_field({alias, Fieldname, _Alias}, Schema) ->
    % BGH: Currently this just allows repeating fields,
    % not actually aliasing them.
    get_field(Fieldname, Schema);
get_field(Fieldname, #schema{}=Schema) ->
    get_field2(Fieldname, Schema#schema.fields).


get_field2(Fieldname, []) ->
    throw({field_not_found, Fieldname});
get_field2(Fieldname, [#field{name=Fieldname}=Field|_]) ->
    Field;
get_field2(Fieldname, [_|Rest]) ->
    get_field2(Fieldname, Rest).


%%%===================================================================
%%% EUnit
%%%===================================================================

-define(TEST_SCHEMA_SIMPLE,
        #schema{
           table = test_table,
           pk = id,
           fields = [#field{ name = id },
                     #field{ name = value },
                     #field{ name = note,
                             codec = json }]
          }).

-define(TEST_SCHEMA_MULTICOL,
        #schema{
           table = test_multicol,
           pk = id,
           fields = [#field{ name = id },
                     #field{ name = vals,
                             columns = [one, two],
                             codec = {
                               fun tuple_to_list/1,
                               fun list_to_tuple/1
                              } }]
          }).


pkget_test() ->
    #dbquery{
       sql = <<"SELECT id, value, note FROM test_table",
               " WHERE (id = $1)">>,
       bindings = [1]
      } = plob_compile:compile(lookup(1, get_obj(?TEST_SCHEMA_SIMPLE))).


where_test() ->
    Query = filter(#{ note => #{ key => <<"something">> },
                      value => <<"Bob">>
                    }, ?TEST_SCHEMA_SIMPLE,
                   get_obj(?TEST_SCHEMA_SIMPLE)),
    #dbquery{
       sql = <<"SELECT id, value, note FROM test_table",
                " WHERE (note = $1 AND value = $2)">>,
       bindings = [<<"{\"key\":\"something\"}">>, <<"Bob">>]
      } = plob_compile:compile(Query).


multicol_select_test() ->
    Query = filter(#{ vals => {a, b} },
                   ?TEST_SCHEMA_MULTICOL,
                   get_obj(?TEST_SCHEMA_MULTICOL)),
    #dbquery{
       sql = <<"SELECT id, one, two FROM test_multicol WHERE (one = $1 AND two = $2)">>,
       bindings = [a, b]
      } = plob_compile:compile(Query).


insert_test() ->
    Schema = ?TEST_SCHEMA_SIMPLE,
    Insert = insert(#{ value => <<"inserted">>,
                       note => #{ foo => <<"bar">> }},
                    Schema),
    #dbquery{
       sql = <<"INSERT INTO test_table (note, value)",
               " VALUES ($1, $2)",
               " RETURNING id">>,
       bindings = [<<"{\"foo\":\"bar\"}">>, <<"inserted">>]
      } = plob_compile:compile(Insert).


update_test() ->
    Schema = ?TEST_SCHEMA_SIMPLE,
    Update = update(#{ value => <<"inserted">>,
                       note => #{ foo => <<"bar">> }},
                    #{ id => 1 },
                    Schema),
    #dbquery{
       sql = <<"UPDATE test_table SET note = $1, value = $2 WHERE (id = $3)">>,
       bindings = [<<"{\"foo\":\"bar\"}">>, <<"inserted">>, 1]
      } = plob_compile:compile(Update).

delete_test() ->
    Schema = ?TEST_SCHEMA_SIMPLE,
    Delete = delete(#{ value => <<"Bob">> }, Schema),
    #dbquery{
       sql = <<"DELETE FROM test_table WHERE (value = $1)">>,
       bindings = [<<"Bob">>]
      } = plob_compile:compile(Delete).

decode_test() ->
    Query = #dbquery{ fields = select_all_fields(?TEST_SCHEMA_SIMPLE) },

    RV = fun(Raw) -> #dbresult{ raw = Raw, module = plob_epgsql } end,

    {error, not_found} = decode_one(Query, RV({ok, []})),

    Found = {ok, [], [{1, <<"foo">>, <<"[1,2,3]">>}]},
    {ok, Row} = decode_one(Query, RV(Found)),
    #{ id := 1,
       note := [1,2,3],
       value := <<"foo">>
     } = Row.

multicol_decode_test() ->
    Query = #dbquery{ fields = select_all_fields(?TEST_SCHEMA_MULTICOL) },
    Result = #dbresult{ raw = {ok, [], [{1, a, b}]}, module = plob_epgsql },
    {ok, Row} = decode_one(Query, Result),
    #{ id := 1,
       vals := {a,b}
     } = Row.
