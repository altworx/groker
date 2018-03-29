%=====================================================================
% Sample server demonstrating usage of groker module

-module(groker).

-behaviour(gen_server).

% API
-export([start_link/0, grok/1, syngrok/1]).

% Gen_server_callbacks
-export([init/1, handle_call/3, handle_cast/2]).

% Tests
-export([test/0]).

-record(state, {pattern :: groklib:exp_pattern()}).

-define(SERVER, ?MODULE).

%=====================================================================
% API and Callbacks

%---------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%---------------------------------------------------------------------
-spec grok(Msg :: string()) -> ok.
grok(Msg) ->
    gen_server:cast(?SERVER, {grok, Msg}).

%---------------------------------------------------------------------
-spec syngrok(Msg :: string()) -> #{atom() => term()}.
syngrok(Msg) ->
    gen_server:call(?SERVER, {grok, Msg}).

%---------------------------------------------------------------------
init(_) ->
    % Core patterns are those uded for expansion of app patterns. Are not expanded anc compiled
    CorePatterns = #{"WORD" => "\\b\\w+\\b",
                     "BASE10NUM" => "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))",
                     "NUMBER" => "(?:%{BASE10NUM})"},

    % Source pattern will be expanded to regular expression
    SourcePattern = "%{WORD:name} is %{WORD:gender}, %{NUMBER:age:int} years old and weighs %{NUMBER:weight:float} kilograms",

    % Extract metadata, expand, compile
    ExpandedPattern = groklib:build_pattern(SourcePattern, CorePatterns),

    {ok, #state{pattern = ExpandedPattern}}.

%---------------------------------------------------------------------
handle_call({grok, Msg}, _From,  #state{pattern = {Metadata, RE}} = State) ->
    Reply = groklib:match(Msg, Metadata, RE),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    {reply, unknown_request, State}.

%---------------------------------------------------------------------
handle_cast(_Request, State) ->
    {noreply, State}.

%=====================================================================
%% Private functions

%=====================================================================
% Tests (will be replaced by common tests)

%---------------------------------------------------------------------
test() ->
    io:format("~p~n", [syngrok("Gary is male, 25 years old and weighs 68.5 kilograms")]),
    io:format("~p~n", [syngrok("Penny is female, 19 years old and weighs 54.0 kilograms")]),
    io:format("~p~n", [syngrok("This message doesn't match")]).

