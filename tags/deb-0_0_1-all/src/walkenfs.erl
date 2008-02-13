-module (walkenfs).
-behaviour (application).
-export ([ start/0,
           start/2,
           stop/0,
           stop/1 ]).

-include ("walkenfs.hrl").

%-=====================================================================-
%-                        application callbacks                        -
%-=====================================================================-

%% @hidden

start () ->
  % we need our own ets to fix the following bug in R11B-5:
  % http://www.erlang.org/pipermail/erlang-bugs/2007-December/000530.html
  % TODO: detect R11B-5 => ets 4.4.5 
  code:ensure_loaded (ets),
  case check_ets_version () of
    n54 -> ok;
    _ ->
      ok = code:unstick_dir (code:lib_dir (stdlib) ++ "/ebin"),
      true = code:soft_purge (ets),
      case code:lib_dir (walkenfs) of
        { error, bad_name } -> % testing, hopefully
          { module, ets } = code:load_file (ets);
        Dir ->
          { module, ets } = code:load_abs (Dir ++ "/ebin/ets")
      end,
      n54 = check_ets_version ()
  end,
  application:load (fuserl),
  application:start (walkenfs).

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
  case lists:keysearch (version, 1, Attributes) of
    { value, { version, [ Version ] } } ->
      Version;
    _ ->
      undefined
  end.
