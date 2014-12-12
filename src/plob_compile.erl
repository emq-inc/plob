%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% SQL compilation
%%% @end
%%% Created : 13 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(plob_compile).

-include("plob.hrl").
-include("plob_compile.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([compile/1]).

%%%===================================================================
%%% Queries
%%%===================================================================

-spec compile(operation()) -> #dbquery{}.
compile(#select{fields=Fields, where=Where, limit=Limit}) ->
    compile_dbquery(
      [<<"SELECT ">>, compile_field_names(Fields),
       <<" FROM ">>, compile_table_names(Fields),
       compile_where(Where),
       case Limit of
           undefined -> <<>>;
           Limit when is_integer(Limit) ->
               LimitBin = integer_to_binary(Limit),
               <<" LIMIT ", LimitBin/binary>>
       end
      ], Fields);

compile(#insert{fields=Fields, return=Return}) ->
    only_one_schema(Fields),
    compile_dbquery(
      [<<"INSERT INTO ">>, compile_table_names(Fields),
       <<" (">>, compile_field_names(Fields),
       <<") VALUES (">>, term_join(prepare_bindings(Fields), <<", ">>),
       <<") RETURNING ">>, compile_field_names(Return)
      ], Return);

compile(#update{fields=Fields, where=Where}) ->
    only_one_schema(Fields),
    only_one_schema(Where),
    same_schema(Fields, Where),
    compile_dbquery(
      [<<"UPDATE ">>, compile_table_names(Fields),
       <<" SET ">>, compile_set(Fields),
       compile_where(Where)
      ], Fields).


-spec only_one_schema(fieldset()) -> ok.
only_one_schema([{_Schema, _}]) -> ok;
only_one_schema(_) -> throw(too_many_schemas).

-spec same_schema(fieldset(), fieldset()) -> ok.
same_schema([{Schema, _}], [{Schema, _}]) -> ok;
same_schema(_, _) -> throw(different_schemas).


-spec compile_dbquery(unbound_query(), fieldset()) -> #dbquery{}.
compile_dbquery(Query, Fields) ->
    {SQL, Bindings} = compile_bindings(Query),
    #dbquery{
       sql = list_to_binary(SQL),
       fields = Fields,
       bindings = Bindings
      }.


%%%===================================================================
%%% Fragments
%%%===================================================================

-spec compile_bindings(unbound_query()) -> {iodata(), [dbval()]}.
compile_bindings(Unbound) ->
    {Query, Bindings, _} =
        lists:foldl(
          fun(#binding{val=Val}, {Q, B, Count}) ->
                  CountBin = integer_to_binary(Count),
                  {[<<"$", CountBin/binary>>|Q],
                   [Val|B], Count+1};
             (Bin, {Q, B, Count}) when is_binary(Bin) ->
                  {[Bin|Q], B, Count}
          end,
          {[], [], 1}, lists:flatten(Unbound)),
    {lists:reverse(Query), lists:reverse(Bindings)}.


-spec compile_table_names(fieldset()) -> iodata().
compile_table_names([{Schema, _Fields}]) ->
    [atom_to_binary(Schema#schema.table, latin1)];
compile_table_names(Other) ->
    %%% TODO: Joins go here
    throw({not_single_table, Other}).


-spec compile_field_names(fieldset()) -> iodata().
compile_field_names(Fieldset) ->
    Names = lists:append(
              [field_colnames(Field)
               || {_Schema, FieldVals} <- Fieldset,
                  {Field, _} <- FieldVals]),
    term_join(Names, <<", ">>).


-spec compile_field_assigns(fieldset()) -> unbound_query().
compile_field_assigns(Fieldset) ->
    [[Binding#binding.col, <<" = ">>, Binding]
     || Binding <- prepare_bindings(Fieldset)].


-spec compile_where(fieldset()) -> unbound_query().
compile_where(Fieldset) ->
    case compile_field_assigns(Fieldset) of
        [] -> [];
        Assigns -> [ <<" WHERE ">> | term_join(Assigns, <<" AND ">>)]
    end.

-spec compile_set(fieldset()) -> unbound_query().
compile_set(Fieldset) ->
    term_join(compile_field_assigns(Fieldset), <<", ">>).


-spec prepare_bindings(fieldset()) -> [#binding{}].
prepare_bindings(Fieldset) ->
    [#binding{ col=Col, val=DBVal }
     || {_Schema, FieldVals} <- Fieldset,
        {Field, ErlVal} <- FieldVals,
        {Col, DBVal} <- lists:zip(field_colnames(Field),
                                  field_values(ErlVal, Field))].

-spec field_colnames(#field{}) -> [binary()].
field_colnames(Field) ->
    [atom_to_binary(Col, latin1)
     || Col <- field_columns(Field)].

-spec field_columns(#field{}) -> columns().
field_columns(#field{name=Name, columns=undefined}) -> [Name];
field_columns(#field{columns=Columns}) when is_list(Columns) -> Columns.

-spec field_values(erlval(), #field{}) -> [dbval()].
field_values(ErlVal, #field{columns=Columns}=Field)
  when is_list(Columns), length(Columns) > 1 ->
    DBVals = plob_codec:encode(Field#field.codec, ErlVal),
    case length(DBVals) =:= length(Columns) of
        true -> DBVals;
        false -> throw({wrong_column_count, Field, DBVals})
    end;
field_values(ErlVal, Field) ->
    [plob_codec:encode(Field#field.codec, ErlVal)].


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
%%% EUnit tests
%%%===================================================================

field_columns_test() ->
    [one] = field_columns(#field{name=one}),
    [one] = field_columns(#field{columns=[one]}),
    [one,two] = field_columns(#field{columns=[one,two]}).

field_values_test() ->
    [val] = field_values(val, #field{name=one}),
    [val] = field_values(val, #field{columns=[one]}),
    [foo, bar] = field_values([foo,bar], #field{columns=[one,two]}).

compile_table_names_test() ->
    [<<"sometable">>] = compile_table_names(
                          [{#schema{table=sometable}, []}]).

compile_field_names_test() ->
    [<<"one">>, <<", ">>, <<"two">>, <<", ">>,  <<"three">>] =
        compile_field_names(
          [{#schema{}, [{#field{name=one}, undefined},
                        {#field{columns=[two, three]}, undefined} ]}]).

prepare_bindings_test() ->
    [#binding{ col= <<"one">>, val=1 },
     #binding{ col= <<"two">>, val=2 },
     #binding{ col= <<"three">>, val=3 }
    ] = prepare_bindings(
          [{#schema{}, [{#field{name=one}, 1},
                        {#field{columns=[two, three]}, [2,3]} ]}]).

compile_bindings_test() ->
    Unbound = [[<<"one">>, <<" = ">>, #binding{ val=1 }], <<", ">>,
               [<<"two">>, <<" = ">>, #binding{ val=2 }], <<", ">>,
               [<<"threefour">>, <<" = ">>, #binding{ val=[3,4] }]],
    Bound = [<<"one">>, <<" = ">>, <<"$1">>, <<", ">>,
             <<"two">>, <<" = ">>, <<"$2">>, <<", ">>,
             <<"threefour">>, <<" = ">>, <<"$3">>],
    Vals = [1, 2, [3, 4]],
    {Bound, Vals} = compile_bindings(Unbound).


compile_where_test() ->
    [<<" WHERE ">>,
     [<<"one">>, <<" = ">>, #binding{ val=1 }], <<" AND ">>,
     [<<"two">>, <<" = ">>, #binding{ val=2 }], <<" AND ">>,
     [<<"three">>, <<" = ">>, #binding{ val=3 }]] =
        compile_where(
          [{#schema{}, [{#field{name=one}, 1},
                        {#field{columns=[two, three]}, [2,3]} ]}]).


    
