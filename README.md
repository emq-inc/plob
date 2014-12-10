Plob
====

Plob lets you define simple Erlang schemas ("models") for DB tables, and then generate SQL queries for them. It also helps you do common things like updating modified fields.


Synopsis (in progress)
----------------------

```erlang
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
```

And then:

```bash
$ erlc -o ebin -I include example/simple.erl && erl -pa ebin -noshell -s simple get_one -s init stop
SQL: SELECT id, value FROM test_table WHERE id = $1
Bindings: [1]
```
