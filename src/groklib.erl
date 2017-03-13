-module(groklib).

-export([get_patterns/2, match/2]).

-define(SPACE, 16#20).
-define(HASH, $#).
-define(BACKSLASH, $\\).

%%====================================================================
%% API functions

%%--------------------------------------------------------------------
%% Returns list of compiled app patterns with their names and metadata
%%
get_patterns(CorePatternDir, AppPatternFile) ->
    CorePatterns = load_patterns_from_dir(CorePatternDir),
    AppPatterns = load_patterns_from_file(AppPatternFile),
    Metadata = extract_metadata(AppPatterns),
    ExpandedPatterns = maps:map(fun(Key, Val) -> {maps:get(Key, Metadata), expand_pattern(Val, CorePatterns)} end, AppPatterns),
    maps:map(fun(_Key, {PatternMetadata, ExpandedPattern}) -> {PatternMetadata, compile_pattern(ExpandedPattern)} end, ExpandedPatterns).

%%--------------------------------------------------------------------
%% Receives text to match and list of compiled patterns.
%% Returns either nomatch or tuple containing name of ther pattern 
%% and list of captured data
%%
-spec match(Text :: list(), Patterns :: list(tuple())) -> nomatch | tuple().

match(_Text, []) ->
    nomatch;

match(Text, [{Name, {Metadata, CompiledPattern}}|T]) ->
    case re:run(Text, CompiledPattern, [global, {capture, all_but_first, list}]) of
        {match, [Captured|_]} ->
            {Name, convert_types(Captured, Metadata)};
        nomatch ->
            match(Text, T)
    end.

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
%% Utility functions for pattern expansion and compilation

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
compile_pattern(P) ->
    {ok, MP} = re:compile(P),
    MP.

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

%%====================================================================
%% Utility functions for meatadata extraction

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
    lists:map(fun([_V, K | _]) -> {list_to_atom(K), undefined} end, Captured).

%%--------------------------------------------------------------------
extract_types(Pattern) ->
    {match, Captured} = re:run(Pattern, "%{(\\w+):(\\w+):(\\w+)}", [ungreedy, global, {capture,all_but_first,list}]),
    lists:map(fun([_V, K, T | _]) -> {list_to_atom(K), list_to_atom(T)} end, Captured).

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

%%====================================================================
%% Utility functions for type conversion

%%--------------------------------------------------------------------
convert_types(Data, Metadata) ->
    convert_types(Data, Metadata, #{}).

convert_types([], [], Result) ->
    Result;

convert_types([Value|Data], [{Name, Type}|Metadata], Result) ->
   convert_types(Data, Metadata, maps:put(Name, convert_type(Type, Value), Result)).

%%--------------------------------------------------------------------
convert_type(int, Val) ->
    list_to_integer(Val);

convert_type(float, Val) ->
    list_to_float(Val);

convert_type(_, Val) ->
    Val.

