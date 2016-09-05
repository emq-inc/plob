%%%-------------------------------------------------------------------
%%% @author Brendon Hogger <brendonh@powder>
%%% @copyright (C) 2014, Brendon Hogger
%%% @doc
%%% Encoders and decoders
%%% @end
%%% Created : 13 Dec 2014 by Brendon Hogger <brendonh@powder>
%%%-------------------------------------------------------------------
-module(plob_codec).

-include("plob.hrl").
-include("plob_compile.hrl").

-export([encode/2,
         decode/2
        ]).


-spec encode(codec(), any()) -> any().
encode({Encoder, _}, Val) -> encode2(Encoder, Val);
encode(Encoder, Val) -> encode2(Encoder, Val).

-spec encode2(encoder(), any()) -> any().
encode2(_Codec, null) -> null;
encode2(undefined, Val) -> Val;
encode2(datetime, Val) -> Val;
encode2(atom, Val) -> atom_to_binary(Val, latin1);
encode2(json, Val) -> jsx:encode(Val);
encode2(Fun, Val) when is_function(Fun) -> Fun(Val);
encode2(Other, _Val) ->
    throw({no_such_encoder, Other}).



-spec decode(codec(), any()) -> any().
decode({_, Decoder}, Val) -> decode2(Decoder, Val);
decode(Decoder, Val) -> decode2(Decoder, Val).

-spec decode2(decoder(), any()) -> any().
decode2(_Codec, null) -> null;
decode2(undefined, Val) -> Val;
%% Round off microseconds as a convenience to make jsx encoder happy.
decode2(datetime, {{_,_,_}=Date, {H,M,SMS}}) -> {Date, {H, M, round(SMS)}};
decode2(json, Val) -> jsx:decode(Val, [{labels, atom}, return_maps]);
decode2(atom, Val) -> binary_to_atom(Val, latin1);
decode2(Fun, Val) when is_function(Fun) -> Fun(Val);
decode2(Other, Val) ->
    io:format("Val: ~p~n", [Val]),
    throw({no_such_decoder, Other}).
