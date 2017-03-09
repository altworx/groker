%%%-------------------------------------------------------------------
%% @doc groker top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(groker_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Policy = {one_for_one, 1, 60},
    Groker = {groker, 
              {groker, start_link, []},
              permanent, 
              brutal_kill,
              worker,
              [groker]},
    
    {ok, {Policy, [Groker]}}.

%%====================================================================
%% Internal functions
%%====================================================================

