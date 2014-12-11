-type fieldname() :: atom().

-type dbval() :: any().
-type erlval() :: any().

-type columns() :: fieldname() | [fieldname()].
-type values() :: erlval() | [erlval()].

-type encoder() :: fun((erlval()) -> dbval()).
-type decoder() :: fun((dbval()) -> erlval()).
-type validator() :: fun((erlval()) -> ok | {error, any()}).

          
-record(field, {
          name :: fieldname(),
          columns :: columns() | undefined,
          default :: erlval(),
          validator :: validator() | undefined,
          encoder :: encoder() | undefined,
          decoder :: decoder() | undefined
         }).

-record(schema, {
          table :: atom(),
          pk :: columns(),
          fields :: [#field{}]
         }).

-record(row, {
          schema :: #schema{},
          existing :: #{ fieldname() => erlval() },
          new :: #{ fieldname() => erlval() }
         }).



-type fieldset() :: [{#schema{}, all | [fieldname()]}].
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
