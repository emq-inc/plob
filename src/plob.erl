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
         get_obj/1,

         lookup/2,
         filter/2,
         compile/1,

         decode_one/2
        ]).


-record(select, {
          fields :: fieldset(),
          where :: where() | undefined,
          limit :: integer() | undefined
         }).

-record(insert, {
          schema :: #schema{},
          fields :: [#field{}],
          values :: [erlval()]
         }).

%%%===================================================================
%%% Select builders
%%%===================================================================

-spec get_obj(#schema{}) -> #select{}.
get_obj(Schema) ->
    #select{ fields = [{Schema, Schema#schema.fields}] }.

-spec lookup(values(), #select{}) -> #dbquery{}.
lookup(PKVal, #select{ fields=[{Schema, _}]}=Select) ->
    Select#select{ where = pk_where(PKVal, Schema) }.

-spec filter(where() | #{}, #select{}) -> #dbquery{}.
filter(#{}=Where, Select) ->
    filter(maps:to_list(Where), Select);
filter(Where, Select) ->
    Select#select{ where = Where }.


%%%===================================================================
%%% Insert / update builders
%%%===================================================================

-spec insert(rowvals(), #schema{}) -> #insert{}.
insert(Map, Schema) ->
    {Fieldnames, Values} = lists:unzip(maps:to_list(Map)),
    Fields = [get_field(Fieldname, Schema) || Fieldname <- Fieldnames],
    #insert{ schema = Schema, fields = Fields, values = Values }.


%%%===================================================================
%%% SQL compilation
%%%===================================================================

%% XXX BGH TODO: This could be a bit nicer, especially around bindings
compile(#select{fields=Fields, where=Where, limit=Limit}) ->
    SQL = list_to_binary(
            [<<"SELECT ">>, sql_col_list(Fields),
             <<" FROM ">>, compile_from(Fields),
             <<" WHERE ">>, compile_where(Where),
             case Limit of
                 undefined -> <<>>;
                 Limit when is_integer(Limit) ->
                     LimitBin = integer_to_binary(Limit),
                     <<" LIMIT ", LimitBin/binary>>
             end
            ]),
    Bindings = bind_where(Where, Fields),
    #dbquery{
       sql = SQL,
       fields = Fields,
       bindings = Bindings
      };

compile(#insert{schema=Schema, fields=Fields, values=Values}) ->
    SQL = list_to_binary(
            [<<"INSERT INTO ">>, atom_to_list(Schema#schema.table),
             <<" (">>, term_join(flat_columns(Fields), <<", ">>),
             <<") VALUES (">>, value_placeholders(length(Fields)),
             <<") RETURNING ">>, sql_pk_list(Schema)
            ]),

    Bindings = [encode_val(Field#field.name, Val, Schema)
                || {Field, Val} <- lists:zip(Fields, Values)],

    #dbquery{
       sql = SQL,
       bindings = Bindings
      }.


%%%===================================================================
%%% Row API
%%%===================================================================

-spec decode_one(#dbquery{}, #dbresult{}) -> {ok, #row{}} | {error, any()}.
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
%%% Encoding and decoding
%%%===================================================================

encode(undefined, Val) -> Val;
encode(json, Val) -> jsx:encode(Val);
encode(Fun, Val) when is_function(Fun) -> Fun(Val);
encode(Other, _Val) ->
    throw({no_such_encoder, Other}).

decode(undefined, Val) -> Val;
decode(json, Val) -> jsx:decode(Val, [{labels, atom}, return_maps]);
decode(Fun, Val) when is_function(Fun) -> Fun(Val);
decode(Other, _Val) ->
    throw({no_such_decoder, Other}).


%%%===================================================================
%%% Internal row functions
%%%===================================================================

-spec list_to_row([dbval()], #dbquery{}) -> #row{}.
% TODO: Join support
list_to_row(List, #dbquery{ fields=[{Schema, Fields}] }) ->
    Values = collect_field_values(Fields, List, #{}),
    #row{ schema=Schema, existing=Values }.

collect_field_values([], [], Result) ->
    Result;
collect_field_values([], MoreCols, _) ->
    throw({too_many_columns, MoreCols});
collect_field_values(Fields, [], _) ->
    throw({not_enough_columns, Fields});
collect_field_values([Field|Rest], Cols, Result) ->
    {DBVal, NewCols} = collect_field_cols(Field, Cols),
    Value = decode(Field#field.decoder, DBVal),
    collect_field_values(Rest, NewCols,
                         maps:put(Field#field.name, Value, Result)).


collect_field_cols(#field{columns=undefined}, [Col|Rest]) ->
    {Col, Rest};
collect_field_cols(#field{columns=Columns}, Cols) ->
    Count = length(Columns),
    case Count of
        1 -> {hd(Cols), tl(Cols)};
        _ -> lists:split(Count, Cols)
    end.


%%%===================================================================
%%% Internal SQL functions
%%%===================================================================

-spec compile_from(fieldset()) -> iodata().
compile_from([{#schema{table=Table}, _}]) ->
    atom_to_list(Table);
% TODO: Joins go here
compile_from(Other) ->
    throw({unsupported_from, Other}).


-spec compile_where(where()) -> iodata().
compile_where(Where) ->
    compile_where(Where, 1, []).

compile_where([], _, Bits) ->
    term_join(Bits, <<" AND ">>, []);
compile_where([{Name, _Val}|Rest], Count, Bits) when is_atom(Name) ->
    Bit = [atom_to_list(Name), <<" = $">>, integer_to_binary(Count)],
    compile_where(Rest, Count+1, [Bit|Bits]).

value_placeholders(Count) ->
    value_placeholders(Count, []).

value_placeholders(0, PHs) ->
    term_join(PHs, <<", ">>);
value_placeholders(Count, PHs) ->
    CountBin = integer_to_binary(Count),
    PH = <<"$", CountBin/binary>>,
    value_placeholders(Count-1, [PH|PHs]).


-spec bind_where(where(), fieldset()) -> [dbval()].
% TODO: Support for join fieldsets
bind_where(Where, [{Schema, _}]) ->
    [encode_val(Fieldname, Val, Schema) || {Fieldname, Val} <- Where].


-spec pk_where(values(), #schema{}) -> where().
pk_where(Val, #schema{pk=PK}) when is_atom(PK) ->
    [{PK, Val}];
pk_where(Vals, #schema{pk=PK}) when is_list(PK) ->
    lists:zip(PK, Vals).

-spec sql_col_list(fieldset()) -> iodata().
sql_col_list(FieldSet) ->
    term_join(sql_col_list(FieldSet, []), <<", ">>).

sql_col_list([], Cols) ->
    lists:flatten(lists:reverse(Cols));
sql_col_list([{_Schema, Fields}|Rest], AllCols) ->
    Cols = flat_columns(Fields),
    sql_col_list(Rest, [Cols|AllCols]).

-spec sql_pk_list(#schema{}) -> iodata().
sql_pk_list(#schema{pk=PK}) when is_atom(PK) ->
    atom_to_list(PK);
sql_pk_list(#schema{pk=PKs}) when is_list(PKs) ->
    term_join([atom_to_list(Col) || Col <- PKs], <<", ">>).


-spec encode_val(fieldname(), erlval(), #schema{}) -> dbval().
encode_val(Fieldname, Val, Schema) ->
    Field = get_field(Fieldname, Schema),
    encode(Field#field.encoder, Val).


%% -spec all_fieldnames(#schema{}) -> [fieldname()].
%% all_fieldnames(#schema{fields=Fields}) ->
%%     [F#field.name || F <- Fields].


-spec flat_columns(#schema{}) -> [columns()].
flat_columns(Fields) ->
    lists:flatmap(
      fun(F) ->
              [list_to_binary(atom_to_list(Col))
               || Col <- field_columns(F)]
      end,
      Fields).

-spec field_columns(#field{}) -> columns().
field_columns(#field{name=Name, columns=undefined}) -> [Name];
field_columns(#field{columns=Columns}) -> Columns.


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
                     #field{ name = value },
                     #field{ name = note,
                             encoder = json,
                             decoder = json }]
          }).


pkget_test() ->
    #dbquery{
       sql = <<"SELECT id, value, note FROM test_table",
               " WHERE id = $1">>,
       bindings = [1]
      } = compile(lookup(1, get_obj(?TEST_SCHEMA_SIMPLE))).

where_test() ->
    #dbquery{
       sql = <<"SELECT id, value, note FROM test_table",
                " WHERE note = $1 AND value = $2">>,
       bindings = [<<"{\"key\":\"something\"}">>, <<"Bob">>]
      } = compile(filter(#{ note => #{ key => <<"something">> },
                            value => <<"Bob">>
                          }, get_obj(?TEST_SCHEMA_SIMPLE))).

decode_test() ->
    Schema = ?TEST_SCHEMA_SIMPLE,
    Query = #dbquery{ fields = [{Schema, Schema#schema.fields}] },
    RV = fun(Raw) -> #dbresult{ raw = Raw, module = plob_epgsql } end,

    {error, not_found} = decode_one(Query, RV({ok, []})),

    Found = {ok, [{1, <<"foo">>, <<"[1,2,3]">>}]},
    {ok, Row} = decode_one(Query, RV(Found)),
    #{ id := 1,
       note := [1,2,3],
       value := <<"foo">> } = Row#row.existing.


insert_test() ->
    Schema = ?TEST_SCHEMA_SIMPLE,
    Insert = insert(#{ value => <<"inserted">>,
                       note => #{ foo => <<"bar">> }}, Schema),
    #dbquery{
       sql = <<"INSERT INTO test_table (note, value)",
               " VALUES ($1, $2)",
               " RETURNING id">>,
       bindings = [<<"{\"foo\":\"bar\"}">>,<<"inserted">>]
      } = compile(Insert).
