-module(groker).

%-export([init/0, match/1]).

-compile(export_all).

-define(SPACE, 16#20).
-define(HASH, $#).
-define(BACKSLASH, $\\).
-define(PATTERN_DIR, "/usr/local/lib/python2.7/site-packages/pygrok/patterns/").

%%--------------------------------------------------------------------
init(Pattern) ->
    Patterns = load_patterns_from_dir(?PATTERN_DIR),
    expand_pattern(Pattern, Patterns).

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

    io:format("Entering high level expansion with ~p~n", [Pattern]),
    Pattern1 = expand_high_level(Pattern, Patterns),

    io:format("Entering low level expansion with: ~p~n", [Pattern1]),
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
            io:format("~p -> ~p~n", [Type, Replacement1]),
            NewPattern = re:replace(Pattern, String, Replacement1, [ungreedy, {return, list}]),
            io:format("~p~n", [NewPattern]),
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
            io:format("~p -> ~p~n", [Type, Replacement1]),
            NewPattern = re:replace(Pattern, String, Replacement1, [ungreedy, {return, list}]),
            io:format("~p~n", [NewPattern]),
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


