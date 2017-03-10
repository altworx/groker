-module(groker).

%-export([init/0, match/1]).

-behaviour(gen_server).

%% API
-export([start_link/0, grok/1, get_pid/0]).

%% Gen_server_callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Tests
-export([test1/0, test2/0, test3/0]).

-record(state, {patterns, metadata, start_time, count}).

-define(SERVER, ?MODULE).
-define(SPACE, 16#20).
-define(HASH, $#).
-define(BACKSLASH, $\\).
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

get_pid() ->
    gen_server:call(?SERVER, getpid).

%%--------------------------------------------------------------------
init(_) ->
    % U expanze vyrazu se pouziva hledani dle klicu, vhodna struktura je mapa
    CorePatterns = load_patterns_from_dir(?PATTERN_DIR),
    AppPatterns = load_patterns_from_file(?PATTERN_FILE),
    Metadata = extract_metadata(AppPatterns),
    io:format("Metadata: ~p~n", [Metadata]),
    ExpPatterns = maps:map(fun(_Key, Val) -> expand_pattern(Val, CorePatterns) end, AppPatterns),

    % Pro zpracovani zprav se ale nehleda dle klicu ale iteruje se po vzorech, vhodnejsi je seznam
    CompPatterns = maps:to_list(maps:map(fun(_Key, Val) -> compile_pattern(Val) end, ExpPatterns)),
    {ok, #state{patterns = CompPatterns, metadata = Metadata, start_time = erlang:timestamp(), count = 0}}.

%%--------------------------------------------------------------------
code_change(_OlvVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
handle_call(getpid, _From, State) ->
    {reply, self(), State};

handle_call(_Request, _From, State) ->
    Reply = unknown_request,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
handle_cast({grok, Msg}, #state{patterns = Patterns, metadata = Metadata, start_time = StartTime, count = Count} = State) ->
    case match(Msg, Patterns) of
        nomatch ->
            io:format("No match~n", []);
        {PatternName, Data} ->
            Output = create_output(PatternName, Data, Metadata),
            io:format("Data: ~p~n", [Output])
    end,

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

handle_cast(Request, State) ->
    io:format("handle_cast - unknown request: ~p~n", [Request]),
    {noreply, State}.

%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%====================================================================
%% Private functions

%---------------------------------------------------------------------
match(_Msg, []) ->
    nomatch;

match(Msg, [{Name, RE}|T]) ->
    case re:run(Msg, RE, [global, {capture, all_but_first, list}]) of
        {match, [Captured|_]} ->
            io:format("Captured: ~p~n", [Captured]),
            {Name, Captured};
        nomatch ->
            match(Msg, T)
    end.

%%--------------------------------------------------------------------
create_output(PatternName, Data, Metadata) ->
    create_output1(Data, maps:get(PatternName, Metadata), #{}).

create_output1([], [], Output) ->
    Output;

create_output1([Value|Data], [{Name, Type}|Metadata], Output) ->
   create_output1(Data, Metadata, maps:put(Name, type(Type, Value), Output)).

%%--------------------------------------------------------------------
type(int, Val) ->
    list_to_integer(Val);

type(float, Val) ->
    list_to_float(Val);

type(_, Val) ->
    Val.

%%--------------------------------------------------------------------
compile_pattern(P) ->
    {ok, MP} = re:compile(P),
    MP.

%%--------------------------------------------------------------------
extract_metadata(Patterns) ->
    L = maps:to_list(Patterns),
    M = extract_metadata(L, []),
    maps:from_list(M).

extract_metadata([], Metadata) ->
    Metadata;

extract_metadata([{Name, Pattern}|Patterns], Metadata) ->
   Names = extract_names(Pattern),
   Types = extract_types(Pattern),
   Merged = merge_names_types(Names, Types),
   extract_metadata(Patterns, [{Name, Merged}|Metadata]).

%%--------------------------------------------------------------------
extract_names(Pattern) ->
    {match, Captured} = re:run(Pattern, "%{(\\w+):(\\w+)(?::\\w+)?}", [ungreedy, global, {capture,all_but_first,list}]),
    lists:map(fun([_V, K | _]) -> {K, undefined} end, Captured).

%%--------------------------------------------------------------------
extract_types(Pattern) ->
    {match, Captured} = re:run(Pattern, "%{(\\w+):(\\w+):(\\w+)}", [ungreedy, global, {capture,all_but_first,list}]),
    lists:map(fun([_V, K, T | _]) -> {K, list_to_atom(T)} end, Captured).

%%--------------------------------------------------------------------
merge_names_types(Names, Types) ->
    merge_names_types(Names, Types, []).

merge_names_types([], _, Merged) ->
    lists:reverse(Merged);

merge_names_types([{Name, Type}|Names], Types, Merged) ->
    T = case get_type(Name, Types) of 
            undefined ->
                Type;
            Tp ->
                Tp
        end,
    merge_names_types(Names, Types, [{Name, T}|Merged]).

%%--------------------------------------------------------------------
get_type(_, []) ->
    undefined;

get_type(Name, [{Name, Type}|_]) ->
    Type;

get_type(Name, [_|Types]) ->
    get_type(Name, Types).

%%--------------------------------------------------------------------
expand_pattern(Pattern, Patterns) ->
    %io:format("Entering high level expansion with ~p~n", [Pattern]),
    Pattern1 = expand_high_level(Pattern, Patterns),

    %io:format("Entering low level expansion with: ~p~n", [Pattern1]),
    Pattern2 = expand_low_level(Pattern1, Patterns),

    case re:run(Pattern2, "%{\\w+(:\\w+)?}", [ungreedy]) of 
        nomatch ->
            Pattern2;
        {match, _} ->
            expand_pattern(Pattern2, Patterns)
    end.

%%--------------------------------------------------------------------
expand_high_level(Pattern, Patterns) ->
    case re:run(Pattern, "%{(\\w+):(\\w+)(?::\\w+)?}", [ungreedy, {capture, all, list}]) of
        nomatch -> 
            Pattern;
        {match, [String, Type, Name|_]} ->
            Replacement = maps:get(Type, Patterns),
            Replacement1 = escape("(?P<" ++ Name ++ ">" ++ Replacement ++ ")"),
            %io:format("~p -> ~p~n", [Type, Replacement1]),
            NewPattern = re:replace(Pattern, String, Replacement1, [ungreedy, {return, list}]),
            %io:format("~p~n", [NewPattern]),
            expand_high_level(NewPattern, Patterns)
    end.

%%--------------------------------------------------------------------
expand_low_level(Pattern, Patterns) ->
     case re:run(Pattern, "%{(\\w+)}", [ungreedy, {capture, all, list}]) of
        nomatch -> 
            Pattern;
        {match, [String, Type|_]} ->
            Replacement = maps:get(Type, Patterns),
            Replacement1 = escape(Replacement),
            %io:format("~p -> ~p~n", [Type, Replacement1]),
            NewPattern = re:replace(Pattern, String, Replacement1, [ungreedy, {return, list}]),
            %io:format("~p~n", [NewPattern]),
            expand_low_level(NewPattern, Patterns)
    end.

%%--------------------------------------------------------------------
escape(Str) ->
    escape(Str, []).

escape([], Rslt) ->
    lists:reverse(Rslt);

escape([H|T], Rslt) ->
    case H =:= ?BACKSLASH of
        true ->
            escape(T, [H | [H | Rslt]]);
        false ->
            escape(T, [H | Rslt])
    end.

%%--------------------------------------------------------------------
load_patterns_from_dir(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            Paths = [lists:append(?PATTERN_DIR, F) || F <- Files],
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
%% Tests

%%--------------------------------------------------------------------
test1() ->
    grok("gary is male, 25 years old and weighs 68.5 kilograms").

%%--------------------------------------------------------------------
test2() ->
    Pid = get_pid(),
    test2(Pid).

test2(Pid) ->
    {message_queue_len, BoxLen} = process_info(Pid, message_queue_len),

    case BoxLen > 2000000 of
        true ->
            io:format("!~n"),
            test2(Pid);
        false ->
            grok("gary is male, 25 years old and weighs 68.5 kilograms"),
            test2(Pid)
    end.

%%--------------------------------------------------------------------
test3() ->
    test3(0).

test3(Cnt) ->
    %% Test 3 ma horsi vysleky nez test 2. Zda se, ze je pro celkovy vykon lepsi
    %% kontinualne plnit mailbox nez ho precpat na zacatku a nechat byt
    grok("gary is male, 25 years old and weighs 68.5 kilograms"),
    case Cnt > 300000 of
        true ->
            io:format("Fin~n", []);
        false ->
            test3(Cnt + 1)
    end.

 
