-type fieldname() :: atom().

-type dbval() :: any().
-type erlval() :: any().

-type columns() :: fieldname() | [fieldname()].
-type values() :: erlval() | [erlval()].

-type encoder() :: atom() | fun((erlval()) -> dbval()).
-type decoder() :: atom() | fun((dbval()) -> erlval()).
-type validator() :: fun((erlval()) -> ok | {error, any()}).

-type rowvals() :: #{ fieldname() => erlval() }.
          
-record(field, {
          name :: fieldname(),
          columns :: columns() | undefined,
          validator :: validator() | undefined,
          encoder :: encoder() | undefined,
          decoder :: decoder() | undefined
         }).

-record(schema, {
          table :: atom(),
          pk :: columns(),
          fields :: [#field{}]
         }).



-type fieldset() :: [{#schema{}, [#field{}]}].
-type where() :: [{fieldname(), erlval()}].
-type sql() :: binary().
-type bindings() :: [dbval()].

-record(dbquery, {
          sql :: binary(),
          fields :: fieldset(),
          bindings :: bindings()
         }).

-record(dbresult, {
          raw :: any(),
          module :: atom()
         }).
