-type fieldname() :: atom() | {alias, atom(), atom()}.

-type dbval() :: any().
-type erlval() :: any().
-type operator() :: binary().

-type columns() :: fieldname() | [fieldname()].
-type values() :: erlval() | [erlval()].

-type encoder() :: atom() | fun((erlval()) -> dbval()).
-type decoder() :: atom() | fun((dbval()) -> erlval()).
-type codec() :: atom() | {encoder(), decoder()}.
-type validator() :: fun((erlval()) -> ok | {error, any()}).

-type rowvals() :: #{ fieldname() => erlval() }.
-type conjugation() :: 'and' | 'or' | 'not'.
-type wherespec() :: rowvals() | {conjugation(), wherespec()}.

-record(field, {
          name :: fieldname(),
          columns :: columns() | undefined,
          validator :: validator() | undefined,
          codec :: codec()
         }).

-record(schema, {
          table :: atom(),
          pk :: columns(),
          fields :: [#field{}]
         }).


-type fieldop() :: {op, operator(), erlval()}.

-type fieldval() :: {#field{}, erlval() | fieldop() | undefined}.
-type fieldvals() :: [{#schema{}, fieldval()}].
-type bindings() :: [dbval()].

-record(whereval, {
          conjugation = 'and' :: conjugation(),
          fieldvals = [] :: [fieldvals() | #whereval{}]
        }).


-record(dbquery, {
          sql :: binary(),
          fields :: fieldvals(),
          bindings :: bindings()
         }).

-record(dbresult, {
          raw :: any(),
          module :: atom()
         }).
