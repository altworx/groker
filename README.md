groker
=====

Erlang implementation of grok pattern matching. Heavily inspired by pygrok (https://github.com/garyelephant/pygrok).

# Sample Usage #
Now all the functionality is in the sigle module:

$ erl
c(groker).
Pattern = "%{WORD:name} is %{WORD:gender}, %{NUMBER:age:int} years old and weighs %{NUMBER:weight:float} kilograms".
Text = "gary is male, 25 years old and weighs 68.5 kilograms".
RE = groker:init(Pattern).
{ok, CRE} = re:compile(RE).
re:run(Text, CRE, [global, {capture, all, list}]).
{match,[["gary is male, 25 years old and weighs 68.5 kilograms",
         "gary","male","25","25","68.5","68.5"]]}

I'll pack it into OTP app next time.

