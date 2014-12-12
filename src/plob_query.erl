%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Simple ORM for PostgreSQL
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

         decode_one/2
        ]).

%%%===================================================================
%%% Select builders
%%%===================================================================

-spec get_obj(#schema{}) -> #select{}.
get_obj(Schema) ->
    #select{ fields = select_all_fields(Schema) }.

-spec lookup(values(), #select{}) -> #select{}.
lookup(PKVal, #select{ fields=[{Schema, _}]}=Select) ->
    Select#select{ where = [pk_where(PKVal, Schema)] }.

-spec filter(rowvals(), #schema{}, #select{}) -> #select{}.
filter(#{}=Map, Schema, Select) ->
    case lists:any(fun({S, _}) -> 
                           S#schema.table =:= Schema#schema.table
                   end,
                   Select#select.fields) of
        true -> ok;
        false -> throw({schema_not_in_fieldset,
                        Schema#schema.table,
                        [S#schema.table || 
                            {S, _} <- Select#select.fields]})
    end,
    Where = map_to_fieldset(Map, Schema),
    Select#select{ where = [Where|Select#select.where] }.


%%%===================================================================
%%% Insert / update builders
%%%===================================================================

-spec insert(rowvals(), #schema{}) -> #insert{}.
insert(Map, Schema) ->
    #insert{ fields = [map_to_fieldset(Map, Schema)],
             return = select_pk_fields(Schema) }.


-spec update(rowvals(), rowvals(), #schema{}) -> #update{}.
update(Vals, Where, Schema) ->
    #update{ fields = [map_to_fieldset(Vals, Schema)],
             where = [map_to_fieldset(Where, Schema)] }.


%% BGH: This is doing too much for the low-level interface.
%% Port it to the object interface later.

%% -spec update(rowvals(), #schema{}) -> #update{}.
%% update(Map, Schema) ->
%%     AllVals = maps:to_list(Map),
%%     PKNames = case Schema#schema.pk of
%%                   Col when is_atom(Col) -> [Col];
%%                   Cols when is_list(Cols) -> Cols
%%               end,
%%     {PKs, Others} = lists:partition(
%%                       fun({Fieldname, _Value}) ->
%%                               lists:member(Fieldname, PKNames)
%%                       end, AllVals),

%%     case length(PKs) =:= length(PKNames) of
%%         true -> ok;
%%         false -> throw(missing_pks)
%%     end,

%%     {Fieldnames, Values} = lists:unzip(Others),
%%     Fields = [get_field(Fieldname, Schema) || Fieldname <- Fieldnames],

%%     #update{
%%        schema = Schema,
%%        fields = Fields,
%%        values = Values,
%%        where = PKs
%%       }.


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
        {error, Error} ->
            {error, Error};
        Other when is_list(Other) ->
            {error, multiple_results};
        Other ->
            throw({error, {unexpected, Other}})
    end.


%%%===================================================================
%%% Internal row functions
%%%===================================================================

-spec list_to_row([dbval()], #dbquery{}) -> rowvals().
% TODO: Join support
list_to_row(List, #dbquery{ fields=[{_Schema, Fields}] }) ->
    collect_field_values(Fields, List, #{}).

-spec collect_field_values([#field{}], [dbval()], rowvals()) -> rowvals().
collect_field_values([], [], Result) ->
    Result;
collect_field_values([], MoreCols, _) ->
    throw({too_many_columns, MoreCols});
collect_field_values(Fields, [], _) ->
    throw({not_enough_columns, Fields});
collect_field_values([{Field,_}|Rest], Cols, Result) ->
    {DBVal, NewCols} = collect_field_cols(Field, Cols),
    Value = plob_codec:decode(Field#field.codec, DBVal),
    collect_field_values(Rest, NewCols,
                         maps:put(Field#field.name, Value, Result)).


collect_field_cols(#field{columns=undefined}, [Col|Rest]) ->
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

-spec pk_where(values(), #schema{}) -> schemavals().
pk_where(Vals, #schema{}=Schema) ->
    {Schema, pk_fieldvals(Vals, Schema)}.

-spec pk_fieldvals(values(), #schema{}) -> [fieldval()].
pk_fieldvals(Val, #schema{pk=PK}=Schema) when is_atom(PK) ->
    [{get_field(PK, Schema), Val}];
pk_fieldvals(Vals, #schema{pk=PK}=Schema) when is_list(PK) ->
    lists:zip([get_field(K, Schema) || K <- PK], Vals).


-spec map_to_fieldset(#{}, #schema{}) -> schemavals().
map_to_fieldset(Map, Schema) ->
    FieldVals = [{get_field(K, Schema), V}
                 || {K, V} <- maps:to_list(Map)],
    {Schema, FieldVals}.


-spec select_all_fields(#schema{}) -> fieldset().
select_all_fields(Schema) ->
    [{Schema, [{F, undefined} || F <- Schema#schema.fields]}].

-spec select_pk_fields(#schema{}) -> fieldset().
select_pk_fields(#schema{pk=PK}=Schema) when is_atom(PK) ->
    [{Schema, [{get_field(PK, Schema), undefined}]}];
select_pk_fields(#schema{pk=PK}=Schema) when is_list(PK) ->
    [{Schema, [{get_field(F, Schema), undefined}
               || F <- PK]}].


-spec get_field(fieldname(), #schema{}) -> #field{}.
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
               " WHERE id = $1">>,
       bindings = [1]
      } = plob_compile:compile(lookup(1, get_obj(?TEST_SCHEMA_SIMPLE))).


where_test() ->
    Query = filter(#{ note => #{ key => <<"something">> },
                      value => <<"Bob">>
                    }, ?TEST_SCHEMA_SIMPLE,
                   get_obj(?TEST_SCHEMA_SIMPLE)),
    #dbquery{
       sql = <<"SELECT id, value, note FROM test_table",
                " WHERE note = $1 AND value = $2">>,
       bindings = [<<"{\"key\":\"something\"}">>, <<"Bob">>]
      } = plob_compile:compile(Query).


multicol_select_test() ->
    Query = filter(#{ vals => {a, b} }, 
                   ?TEST_SCHEMA_MULTICOL,
                   get_obj(?TEST_SCHEMA_MULTICOL)),
    #dbquery{
       sql = <<"SELECT id, one, two FROM test_multicol WHERE one = $1 AND two = $2">>,
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
       sql = <<"UPDATE test_table SET note = $1, value = $2 WHERE id = $3">>,
       bindings = [<<"{\"foo\":\"bar\"}">>, <<"inserted">>, 1]
      } = plob_compile:compile(Update).


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
    
    
