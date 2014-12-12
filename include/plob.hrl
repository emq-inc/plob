-type fieldname() :: atom().

-type dbval() :: any().
-type erlval() :: any().

-type columns() :: fieldname() | [fieldname()].
-type values() :: erlval() | [erlval()].

-type encoder() :: atom() | fun((erlval()) -> dbval()).
-type decoder() :: atom() | fun((dbval()) -> erlval()).
-type codec() :: atom() | {encoder(), decoder()}.
-type validator() :: fun((erlval()) -> ok | {error, any()}).

-type rowvals() :: #{ fieldname() => erlval() }.
          
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


-type fieldval() :: {#field{}, erlval() | undefined}.
-type schemavals() :: {#schema{}, [fieldval()]}.
-type fieldset() :: [schemavals()].
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


