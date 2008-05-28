-module (walkenfs).
-behaviour (application).
-export ([ start/0,
           start/1,
           start/2,
           stop/0,
           stop/1 ]).

-include ("walkenfs.hrl").

%-=====================================================================-
%-                        application callbacks                        -
%-=====================================================================-

%% @hidden

start () ->
  start (permanent).

start (Type) ->
  application:start (fuserl),
  application:start (walkenfs, Type).

%% @hidden

start (_Type, _Args) ->
  { ok, LinkedIn } = application:get_env (walkenfs, linked_in),
  { ok, MountOpts } = application:get_env (walkenfs, mount_opts),
  { ok, MountPoint } = application:get_env (walkenfs, mount_point),
  { ok, Prefix } = application:get_env (walkenfs, prefix),
  { ok, ReadDataContext } = application:get_env (walkenfs, read_data_context),
  { ok, WriteDataContext } = application:get_env (walkenfs, write_data_context),
  { ok, ReadMetaContext } = application:get_env (walkenfs, read_meta_context),
  { ok, WriteMetaContext } = application:get_env (walkenfs, write_meta_context),
  { ok, AttrTimeoutMs } = application:get_env (walkenfs, attr_timeout_ms),
  { ok, EntryTimeoutMs } = application:get_env (walkenfs, entry_timeout_ms),
  { ok, ReuseNode } = application:get_env (walkenfs, reuse_node),
  { ok, StartTimeout } = application:get_env (walkenfs, start_timeout),
  { ok, StopTimeout } = application:get_env (walkenfs, stop_timeout),
  { ok, InitFragments } = application:get_env (walkenfs, init_fragments),
  { ok, InitCopies } = application:get_env (walkenfs, init_copies),
  { ok, CopyType } = application:get_env (walkenfs, copy_type),
  { ok, BuggyEts } = application:get_env (walkenfs, buggy_ets),

  case BuggyEts of
    X when (X =:= true) or (X =:= auto_detect) ->
      % we need our own ets to fix the following bug in R11B-5:
      % http://www.erlang.org/pipermail/erlang-bugs/2007-December/000530.html
      % TODO: detect R11B-5 => ets 4.4.5 
      code:ensure_loaded (ets),
      case check_ets_version () of
        Y when ((X =:= true) and (Y =/= n54)) or 
               ((X =:= auto_detect) and 
                (Y =:= 321717412589622444982284875105537266341)) ->
          error_logger:info_msg ("walkenfs attempting load of ets: ~p ~p~n",
                                 [ X, Y ]),
          ok = code:unstick_dir (code:lib_dir (stdlib) ++ "/ebin"),
          true = code:soft_purge (ets),
          case code:priv_dir (walkenfs) of
            { error, bad_name } -> % testing, hopefully
              { module, ets } = code:load_file (ets);
            Dir ->
              { module, ets } = code:load_abs (Dir ++ "/ets")
          end,
          n54 = check_ets_version ();
        V ->
          error_logger:info_msg ("walkenfs not attempting load of ets: ~p ~p~n",
                                 [ X, V ]),
          ok
      end;
    false ->
      ok
  end,

  walkenfssup:start_link (LinkedIn,
                          MountOpts, 
                          MountPoint, 
                          Prefix,
                          ReadDataContext,
                          WriteDataContext,
                          ReadMetaContext,
                          WriteMetaContext,
                          AttrTimeoutMs,
                          EntryTimeoutMs,
                          ReuseNode,
                          StartTimeout,
                          StopTimeout,
                          InitFragments,
                          InitCopies,
                          CopyType).

%% @hidden

stop () ->
  application:stop (walkenfs).

%% @hidden

stop (_State) ->
  ok.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

check_ets_version () ->
  { value, { attributes, Attributes } } = lists:keysearch (attributes,
                                                           1,
                                                           ets:module_info ()),
  case lists:keysearch (vsn, 1, Attributes) of
    { value, { vsn, [ Version ] } } ->
      Version;
    _ ->
      undefined
  end.
