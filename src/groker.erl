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

-record(state, {patterns :: #{string() => groklib:exp_pattern()}, start_time :: erlang:timastamp(), count :: non_neg_integer()}).

-define(SPACE, 16#20).
-define(HASH, $#).
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
-spec grok(Msg :: string()) -> ok.

grok(Msg) -> 
    gen_server:cast(?SERVER, {grok, Msg}).

%%--------------------------------------------------------------------
-spec syngrok(Msg :: string()) -> #{atom() => term()}.

syngrok(Msg) ->
    gen_server:call(?SERVER, {grok, Msg}).

%%--------------------------------------------------------------------
-spec get_pid() -> pid().

get_pid() ->
    gen_server:call(?SERVER, getpid).

%%--------------------------------------------------------------------
init(_) ->
    % Core patterns are those uded for expansion of app patterns. Are not expanded anc compiled
    CorePatterns = load_patterns_from_dir(?PATTERN_DIR),

    % App patterns are expanded with core patterns. They are compiled after axpansion and used for patterns matching.
    AppPatterns = load_patterns_from_file(?PATTERN_FILE),

    % Extract metadata, expand, compile
    Patterns = maps:map(fun(_Key, Pattern) -> groklib:build_pattern(Pattern, CorePatterns) end, AppPatterns),

    {ok, #state{patterns = Patterns, start_time = erlang:timestamp(), count = 0}}.

%%--------------------------------------------------------------------
code_change(_OlvVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
handle_call(getpid, _From, State) ->
    {reply, self(), State};

handle_call({grok, Msg}, _From,  #state{patterns = Patterns} = State) ->
    {Metadata, RE} = maps:get("PATTERN1", Patterns),
    Reply = groklib:match(Msg, Metadata, RE),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    Reply = unknown_request,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
handle_cast({grok, Msg}, #state{patterns = Patterns, start_time = StartTime, count = Count} = State) ->
    {Metadata, RE} = maps:get("PATTERN1", Patterns),
    groklib:match(Msg, Metadata, RE),

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
%% Private functions

%%====================================================================
%% Utility functions for loading patterns from files

%%--------------------------------------------------------------------
load_patterns_from_dir(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            Paths = [lists:append(Dir, F) || F <- Files],
            load_patterns_from_files(Paths, #{});
        _ ->
            #{}
    end.

%%--------------------------------------------------------------------
load_patterns_from_files([], Patterns) ->
    Patterns;

load_patterns_from_files([Path|Paths], Patterns) ->
    NewPatterns = maps:merge(Patterns, load_patterns_from_file(Path)),
    load_patterns_from_files(Paths, NewPatterns).

%%--------------------------------------------------------------------
load_patterns_from_file(Path) ->
    {ok, File} = file:open(Path, [read]),
    process_file(File).

%%--------------------------------------------------------------------
process_file(File) ->
    process_file(File, #{}).

process_file(File, Patterns) ->
    case file:read_line(File) of
        eof -> 
            Patterns;
        {ok, Line} ->
            Rslt = process_line(Line),
            case Rslt of
                nopattern ->
                    process_file(File, Patterns);
                {pattern, Key, Val} ->
                    NewPatterns = maps:put(Key, Val, Patterns),
                    process_file(File, NewPatterns)
            end
    end.

%%--------------------------------------------------------------------
process_line(Line) ->
    Line1 = string:strip(Line, right, $\n),
    Line2 = string:strip(Line1),
    case length(Line2) of
        0 ->
            % Empty line
            nopattern;
        _ ->
            [F| _] = Line2,
            case F of
                ?HASH ->
                    % Comment
                    nopattern;
                _ ->
                    % First space char separates pattern name from pattern
                    Pos = string:chr(Line2, ?SPACE),
                    case Pos of
                        0 ->
                            % Invalid line
                            nopattern;
                        _ ->
                            % Valid line
                            Key = string:substr(Line2, 1, Pos - 1),
                            Val = string:substr(Line2, Pos + 1),
                            {pattern, string:strip(Key), string:strip(Val)}
                    end
            end
    end.

%%====================================================================
%% Tests (will be replaced by common tests)

%%--------------------------------------------------------------------
match_test() ->
    io:format("~p~n", [syngrok(message1())]),
    io:format("~p~n", [syngrok(message2())]),
    io:format("~p~n", [syngrok(message3())]),
    io:format("~p~n", [syngrok(message4())]),
    io:format("~p~n", [syngrok(message5())]).

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
            grok(message4()),
            benchmark1(Pid)
    end.

%%--------------------------------------------------------------------
benchmark2() ->
    benchmark2(0).

benchmark2(Cnt) ->
    grok(message1()),
    case Cnt > 300000 of
        true ->
            io:format("Fin~n", []);
        false ->
            benchmark2(Cnt + 1)
    end.

%%--------------------------------------------------------------------
message1() ->
    "Gary is male, 25 years old and weighs 68.5 kilograms".

message2() ->
    "Penny is female, 19 years old and weighs 54.0 kilograms".

message3() ->
    "This message doesn't match".

message4() ->
    "Mar 17 16:52:03 td2 180768781,,,,,1,,,win_sec-4768-ux-success,ntto209,,1489258562,Undefined,,,,,,,,,,,,,,,,auth.aaa.login.grant,,zu002676,25,,,10.198.80.86,,,58326,,o2.srv.nt.prod,,,,,,,,,Undefined,,,,,20170317-150755921".

message5() ->
    "Mar 17 16:52:03 td2 180768781,,,,,1,,,win_sec-4768-ux-success,ntto209,,1489258562,Undefined,,,,,,,,,,,,,,,,auth.aaa.login.deny,,zu002676,25,,,10.198.80.86,,,58326,,o2.srv.nt.prod,,,,,,,,,Undefined,,,,,20170317-150755921".


