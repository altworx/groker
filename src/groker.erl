-module(groker).

%-export([init/0, match/1]).

-behaviour(gen_server).

%% API
-export([start_link/0, grok/1, get_pid/0]).

%% Gen_server_callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Tests
-export([test1/0, test2/0]).

-record(state, {patterns, start_time, count}).

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
    %io:format("grok message ~p~n", [Msg]),
    gen_server:cast(?SERVER, {grok, Msg}).

get_pid() ->
    gen_server:call(?SERVER, getpid).

%%--------------------------------------------------------------------
init(_) ->
    % U expanze vyrazu se pouziva hledani dle klicu, vhodna struktura je mapa
    CorePatterns = load_patterns_from_dir(?PATTERN_DIR),
    AppPatterns = load_patterns_from_file(?PATTERN_FILE),
    ExpPatterns = maps:map(fun(_Key, Val) -> expand_pattern(Val, CorePatterns) end, AppPatterns),

    % Pro zpracovani zprav se ale nehleda dle klicu ale iteruje se po vzorech, vhodnejsi je seznam
    CompPatterns = maps:to_list(maps:map(fun(_Key, Val) -> compile_pattern(Val) end, ExpPatterns)),
    %io:format("CompPatterns: ~p~n", [CompPatterns]),
    {ok, #state{patterns = CompPatterns, start_time = erlang:timestamp(), count = 0}}.

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
handle_cast({grok, Msg}, #state{patterns = Patterns, start_time = StartTime, count = Count} = State) ->
    %io:format("handle_cast: ~p~n", [Msg]),
    _Data = match(Msg, Patterns),
    %io:format("Data: ~p~n", [Data]),

    NewCount = Count + 1, 
    case NewCount =:= ?LIMIT of
        true ->
            {message_queue_len, BoxLen} = process_info(self(), message_queue_len),
            Seconds = timer:now_diff(erlang:timestamp(), StartTime) / 1000000,
            Rate = ?LIMIT / Seconds,
            io:format("rate: ~p, box len: :~p~n", [Rate, BoxLen]),
            %io:format("~p~n", [Rate]),
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
    %io:format("match - H: ~p~n", [RE]),
    case re:run(Msg, RE, [global, {capture, all_names, list}]) of
        {match, Captured} ->
            {Name, Captured};
        nomatch ->
            match(Msg, T)
    end.

%%--------------------------------------------------------------------
compile_pattern(P) ->
    {ok, MP} = re:compile(P),
    MP.

%%--------------------------------------------------------------------
expand_pattern(Pattern, Patterns) ->
    expand_pattern(Pattern, Patterns, #{}).

expand_pattern(Pattern, Patterns, DataTypes) ->
    NewTypes = case re:run(Pattern, "%{(\\w+):(\\w+):(\\w+)}", [ungreedy, global, {capture,all_but_first,list}]) of
                   {match, Types} ->
                       maps:merge(DataTypes, extract_types(Types));
                   nomatch ->
                       DataTypes
               end,

    %io:format("Entering high level expansion with ~p~n", [Pattern]),
    Pattern1 = expand_high_level(Pattern, Patterns),

    %io:format("Entering low level expansion with: ~p~n", [Pattern1]),
    Pattern2 = expand_low_level(Pattern1, Patterns),

    case re:run(Pattern2, "%{\\w+(:\\w+)?}", [ungreedy]) of 
        nomatch ->
            Pattern2;
        {match, _} ->
            expand_pattern(Pattern2, Patterns, NewTypes)
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
extract_types(TypeGroups) ->
    extract_types(TypeGroups, #{}).

extract_types([], Types) ->
    Types;

extract_types([TypeGroup|TypeGroups], Types) ->
    [_, Name, Type|_] = TypeGroup,
    NewTypes = maps:put(Name, Type, Types),
    extract_types(TypeGroups, NewTypes).

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

test1() ->
    Pid = get_pid(),
    test1(Pid).

test1(Pid) ->
    {message_queue_len, BoxLen} = process_info(Pid, message_queue_len),

    case BoxLen > 2000000 of
        true ->
            io:format("!~n"),
            test1(Pid);
        false ->
            grok("gary is male, 25 years old and weighs 68.5 kilograms"),
            test1(Pid)
    end.

test2() ->
    test2(0).

test2(Cnt) ->
    %% Test 2 ma horsi vysleky nez test 1. Zda se, ze je pro celkovy vykon lepsi
    %% kontinualne plnit mailbox nez ho precpat na zacatku a nechat byt
    grok("gary is male, 25 years old and weighs 68.5 kilograms"),
    case Cnt > 300000 of
        true ->
            io:format("Fin~n", []);
        false ->
            test2(Cnt + 1)
    end.

 
