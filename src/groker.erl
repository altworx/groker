%%====================================================================
%% Sample server demonstrating usage of groker module

-module(groker).

-behaviour(gen_server).

%% API
-export([start_link/0, grok/1, syngrok/1, get_pid/0]).

%% Gen_server_callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Tests
-export([match_test/0, benchmark1/0, benchmark2/0]).

-record(state, {patterns, start_time, count}).

-define(SERVER, ?MODULE).
-define(PATTERN_DIR, "./config/patterns/").
-define(PATTERN_FILE, "./config/syslog_patterns").
-define(LIMIT, 100000).

%%====================================================================
%% API and Callbacks

%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
grok(Msg) -> 
    gen_server:cast(?SERVER, {grok, Msg}).

syngrok(Msg) ->
    gen_server:call(?SERVER, {grok, Msg}).

get_pid() ->
    gen_server:call(?SERVER, getpid).

%%--------------------------------------------------------------------
init(_) ->
    Patterns = groklib:get_patterns(?PATTERN_DIR, ?PATTERN_FILE),
    % Patterns is a map but message processing doesn't use keys. Instead it iterates through patterns. List is better structure.
    {ok, #state{patterns = maps:to_list(Patterns), start_time = erlang:timestamp(), count = 0}}.

%%--------------------------------------------------------------------
code_change(_OlvVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
handle_call(getpid, _From, State) ->
    {reply, self(), State};

handle_call({grok, Msg}, _From,  #state{patterns = Patterns} = State) ->
    Reply = groklib:match(Msg, Patterns),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    Reply = unknown_request,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
handle_cast({grok, Msg}, #state{patterns = Patterns, start_time = StartTime, count = Count} = State) ->
    groklib:match(Msg, Patterns),

    NewCount = Count + 1,
    case NewCount =:= ?LIMIT of
        true ->
            {message_queue_len, BoxLen} = process_info(self(), message_queue_len),
            Seconds = timer:now_diff(erlang:timestamp(), StartTime) / 1000000,
            Rate = ?LIMIT / Seconds,
            io:format("rate: ~p, box len: :~p~n", [Rate, BoxLen]),
            {noreply, State#state{start_time = erlang:timestamp(), count = 0}}; 
        false ->
            {noreply, State#state{count = NewCount}}
    end;  

handle_cast(_Request, State) ->
    % Uknown request. Don't bother.
    {noreply, State}.

%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    io:format("Terminated~n", []).

%%====================================================================
%% Tests (will be replaced by common tests)

%%--------------------------------------------------------------------
match_test() ->
    io:format("~p~n", [syngrok("Gary is male, 25 years old and weighs 68.5 kilograms")]),
    io:format("~p~n", [syngrok("Penny is female, 19 years old and weighs 54.0 kilograms")]),
    io:format("~p~n", [syngrok("This message doesn't match")]).

%%--------------------------------------------------------------------
benchmark1() ->
    Pid = get_pid(),
    benchmark1(Pid).

benchmark1(Pid) ->
    {message_queue_len, BoxLen} = process_info(Pid, message_queue_len),

    case BoxLen > 2000000 of
        true ->
            io:format("!~n"),
            benchmark1(Pid);
        false ->
            grok("gary is male, 25 years old and weighs 68.5 kilograms"),
            benchmark1(Pid)
    end.

%%--------------------------------------------------------------------
benchmark2() ->
    benchmark2(0).

benchmark2(Cnt) ->
    grok("gary is male, 25 years old and weighs 68.5 kilograms"),
    case Cnt > 300000 of
        true ->
            io:format("Fin~n", []);
        false ->
            benchmark2(Cnt + 1)
    end.

