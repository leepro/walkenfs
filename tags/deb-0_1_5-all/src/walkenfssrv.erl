-module (walkenfssrv).
-export ([ start_link/11 ]).
%-behavior (fuserl).
-export ([ init/1,
           code_change/3,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           access/5,
           create/7,
           flush/5,
           forget/5,
           fsync/6,
           fsyncdir/6,
           getattr/4,
           getxattr/6,
           link/6,
           listxattr/5,
           lookup/5,
           mkdir/6,
           mknod/7,
           open/5,
           opendir/5,
           read/7,
           readdir/7,
           readlink/4,
           release/5,
           releasedir/5,
           removexattr/5,
           rename/7,
           rmdir/5,
           unlink/5,
           setattr/7,
           setxattr/7,
           statfs/4,
           symlink/6,
           write/7 ]).

-include_lib ("fuserl/include/fuserl.hrl").
-include ("walkenfs.hrl").

-ifdef (HAVE_EUNIT).
-include_lib ("eunit/include/eunit.hrl").
-include_lib ("kernel/include/file.hrl").
-endif.

-ifdef (FLASS).
-define (YO (X, Y), io:format (user, X, Y)).
-else.
-define (YO (X, Y), ok).
-endif.

-define (is_bool (X), (((X) =:= true) or ((X) =:= false))).
-define (is_string_like (X), (is_list (X) and 
                              (length (X) =:= 0 orelse is_integer (hd (X))))).
-define (is_context (X), (((X) =:= async_dirty) or
                          ((X) =:= sync_dirty) or
                          ((X) =:= transaction) or
                          ((X) =:= sync_transaction))).

-record (state, { attr_timeout_ms,
                  block_size_bytes = 512,       % TODO: make configurable
                  block_map_table,
                  block_store_table,
                  directory_entry_table,
                  entry_timeout_ms,
                  inode_table,
                  meta_table,
                  read_data_context,
                  read_meta_context,
                  symlink_table,
                  write_data_context,
                  write_meta_context,
                  xattr_table }).

-define (walkenfs_error_msg (A, B), 
         error_logger:error_msg ("~p ~p: " ++ A, [ ?FILE, ?LINE ] ++ B)).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link (LinkedIn, 
            MountOpts, 
            MountPoint, 
            Prefix,
            ReadDataContext,
            WriteDataContext,
            ReadMetaContext,
            WriteMetaContext,
            AttrTimeoutMs,
            EntryTimeoutMs,
            Options) 
  when ?is_bool (LinkedIn),
       ?is_string_like (MountOpts),
       ?is_string_like (MountPoint),
       is_atom (Prefix),
       ?is_context (ReadDataContext),
       ?is_context (WriteDataContext),
       ?is_context (ReadMetaContext),
       ?is_context (WriteMetaContext),
       is_integer (AttrTimeoutMs), AttrTimeoutMs >= 0,
       is_integer (EntryTimeoutMs), EntryTimeoutMs >= 0,
       is_list (Options) ->
  fuserlsrv:start_link (?MODULE,
                        LinkedIn,
                        MountOpts,
                        MountPoint,
                        [ Prefix, ReadDataContext, WriteDataContext,
                          ReadMetaContext, WriteMetaContext, AttrTimeoutMs,
                          EntryTimeoutMs ],
                        Options).

%-=====================================================================-
%-                           fuserl callbacks                          -
%-=====================================================================-

%% @hidden

init ([ Prefix,
        ReadDataContext,
        WriteDataContext,
        ReadMetaContext,
        WriteMetaContext,
        AttrTimeoutMs,
        EntryTimeoutMs ]) ->
  BlockMapTable = append_atom (Prefix, "_block_map"),
  BlockStoreTable = append_atom (Prefix, "_block_store"),
  DirectoryEntryTable = append_atom (Prefix, "_directory_entry"),
  InodeTable = append_atom (Prefix, "_inode"),
  MetaTable = append_atom (Prefix, "_meta"),
  SymlinkTable = append_atom (Prefix, "_symlink"),
  XattrTable = append_atom (Prefix, "_xattr"),

  { ok, #state{ attr_timeout_ms = AttrTimeoutMs,
                block_map_table = BlockMapTable,
                block_store_table = BlockStoreTable,
                directory_entry_table = DirectoryEntryTable,
                entry_timeout_ms = EntryTimeoutMs,
                inode_table = InodeTable,
                meta_table = MetaTable,
                read_data_context = ReadDataContext,
                read_meta_context = ReadMetaContext,
                symlink_table = SymlinkTable,
                write_data_context = WriteDataContext,
                write_meta_context = WriteMetaContext,
                xattr_table = XattrTable } }.

handle_call (_Request, _From, State) -> { noreply, State }.
handle_cast (_Request, State) -> { noreply, State }.
handle_info ({ _, { exit_status, 0 } }, State) ->
  case application:get_env (walkenfs, port_exit_stop) of
    { ok, true } ->
      % well, this is an unmount(8) ... (probably?)
      spawn (fun () -> init:stop () end),
      { stop, normal, State };
    _ ->
      { noreply, State }
  end;
handle_info (_Msg, State) -> 
  { noreply, State }.
code_change (_OldVsn, State, _Extra) -> { ok, State }.
terminate (_Reason, _State) -> ok.

%% @hidden

access (Ctx, Inode, Mask, Cont, State) ->
  ?YO ("access~n", []),
  spawn_link (fun () -> access_async (Ctx, Inode, Mask, Cont, State) end),
  { noreply, State }.

%% @hidden

create (Ctx, ParentInode, Name, Mode, Fi, Cont, State) ->
  ?YO ("create~n", []),
  spawn_link 
    (fun () -> 
       create_async (Ctx, ParentInode, Name, Mode, Fi, Cont, State)
     end),
  { noreply, State }.

%% @hidden

flush (_Ctx, _Inode, _Fi, _Cont, State) ->
  ?YO ("flush~n", []),
  { #fuse_reply_err{ err = ok }, State }.

%% @hidden

forget (_Ctx, _Inode, _Nlookup, _Cont, State) ->
  ?YO ("forget~n", []),
  { #fuse_reply_none{}, State }.

%% @hidden

fsync (_Ctx, _Inode, _IsDataSync, _Fi, _Cont, State) ->
  ?YO ("fsync~n", []),
  { #fuse_reply_err{ err = ok }, State }.

%% @hidden

fsyncdir (_Ctx, _Inode, _IsDataSync, _Fi, _Cont, State) ->
  ?YO ("fsyncdir~n", []),
  { #fuse_reply_err{ err = ok }, State }.

%% @hidden

getattr (Ctx, Inode, Cont, State) ->
  ?YO ("getattr~n", []),
  spawn_link (fun () -> getattr_async (Ctx, Inode, Cont, State) end),
  { noreply, State }.

%% @hidden

getxattr (_Ctx, _Inode, <<"system.", _/binary>>, _Size, _Cont, State) ->
  { #fuse_reply_err{ err = enotsup }, State };
getxattr (Ctx, Inode, Name, Size, Cont, State) ->
  ?YO ("getxattr~n", []),
  spawn_link (fun () -> getxattr_async (Ctx,
                                        Inode,
                                        Name,
                                        Size,
                                        Cont,
                                        State) 
              end),
  { noreply, State }.

%% @hidden

link (Ctx, Inode, NewParent, NewName, Cont, State) ->
  ?YO ("link~n", []),
  spawn_link 
    (fun () -> link_async (Ctx, Inode, NewParent, NewName, Cont, State) end),
  { noreply, State }.

%% @hidden

listxattr (Ctx, Inode, Size, Cont, State) ->
  ?YO ("listxattr~n", []),
  spawn_link (fun () -> listxattr_async (Ctx, Inode, Size, Cont, State) end),
  { noreply, State }.

%% @hidden

lookup (Ctx, Parent, Name, Cont, State) ->
  ?YO ("lookup~n", []),
  spawn_link (fun () -> lookup_async (Ctx, Parent, Name, Cont, State) end),
  { noreply, State }.

%% @hidden

mkdir (Ctx, ParentInode, Name, Mode, Cont, State) ->
  ?YO ("mkdir~n", []),
  mknod (Ctx, ParentInode, Name, Mode bor ?S_IFDIR, { 0, 0 }, Cont, State).

%% @hidden

% TODO: support named pipes.  that'd be a neat trick since the two ends
% could be opened on different machines.

mknod (Ctx, ParentInode, Name, Mode, Dev, Cont, State) ->
  ?YO ("mknod~n", []),
  case Mode band ?S_IFMT of
    N when ((N =:= ?S_IFLNK) or 
            (N =:= ?S_IFREG) or 
            (N =:= ?S_IFDIR)) ->
      spawn_link 
        (fun () -> 
           mknod_async (Ctx, ParentInode, Name, Mode, Dev, Cont, State) 
         end),
             
      { noreply, State };
    _ ->
      { #fuse_reply_err{ err = enotsup }, State }
  end.

%% @hidden

open (Ctx, Inode, Fi, Cont, State) ->
  ?YO ("open~n", []),
  spawn_link (fun () -> open_async (Ctx, Inode, Fi, Cont, State) end),
  { noreply, State }.

%% @hidden

opendir (Ctx, Inode, Fi, Cont, State) ->
  ?YO ("opendir~n", []),
  spawn_link (fun () -> open_async (Ctx, Inode, Fi, Cont, State) end),
  { noreply, State }.

%% @hidden

read (Ctx, Inode, Size, Offset, Fi, Cont, State) ->
  ?YO ("read~n", []),
  spawn_link (fun () -> read_async (Ctx,
                                    Inode,
                                    Size,
                                    Offset,
                                    Fi,
                                    Cont,
                                    State)
              end),
  { noreply, State }.

%% @hidden

readdir (Ctx, Inode, Size, Offset, Fi, Cont, State) ->
  ?YO ("readdir~n", []),
  spawn_link (fun () -> readdir_async (Ctx,
                                       Inode,
                                       Size,
                                       Offset,
                                       Fi,
                                       Cont,
                                       State)
              end),
  { noreply, State }.

%% @hidden

readlink (Ctx, Inode, Cont, State) ->
  ?YO ("readlink~n", []),
  spawn_link (fun () -> readlink_async (Ctx, Inode, Cont, State) end),
  { noreply, State }.

%% @hidden

release (_Ctx, _Inode, _Fi, _Cont, State) ->
  ?YO ("release~n", []),
  { #fuse_reply_err{ err = ok }, State }.

%% @hidden

releasedir (_Ctx, _Inode, _Fi, _Cont, State) ->
  ?YO ("releasedir~n", []),
  { #fuse_reply_err{ err = ok }, State }.

%% @hidden

removexattr (Ctx, Inode, Name, Cont, State) ->
  ?YO ("removexattr~n", []),
  spawn_link (fun () -> removexattr_async (Ctx, Inode, Name, Cont, State) end),
  { noreply, State }.

%% @hidden

rename (Ctx, Parent, Name, NewParent, NewName, Cont, State) ->
  ?YO ("rename~n", []),
  spawn_link (fun () -> rename_async (Ctx,
                                      Parent,
                                      Name,
                                      NewParent,
                                      NewName,
                                      Cont,
                                      State) 
              end),
  { noreply, State }.

%% @hidden

rmdir (Ctx, ParentInode, Name, Cont, State) ->
  ?YO ("rmdir~n", []),
  spawn_link (fun () -> rmdir_async (Ctx, ParentInode, Name, Cont, State) end),
  { noreply, State }.

%% @hidden

setxattr (Ctx, Inode, Name, Value, Flags, Cont, State) ->
  ?YO ("setxattr~n", []),
  spawn_link (fun () -> setxattr_async (Ctx,
                                        Inode,
                                        Name,
                                        Value,
                                        Flags,
                                        Cont,
                                        State) 
              end),
  { noreply, State }.

%% @hidden

unlink (Ctx, Parent, Name, Cont, State) ->
  ?YO ("unlink~n", []),
  spawn_link (fun () -> unlink_async (Ctx, Parent, Name, Cont, State) end),
  { noreply, State }.

%% @hidden

setattr (Ctx, Inode, Attr, ToSet, Fi, Cont, State) ->
  ?YO ("setattr~n", []),
  spawn_link (fun () -> setattr_async (Ctx,
                                       Inode,
                                       Attr,
                                       ToSet,
                                       Fi,
                                       Cont,
                                       State)
              end),
  { noreply, State }.

%% @hidden

statfs (Ctx, Inode, Cont, State) ->
  ?YO ("statfs~n", []),
  spawn_link (fun () -> statfs_async (Ctx, Inode, Cont, State) end),
  { noreply, State }.

%% @hidden

symlink (Ctx, Link, Inode, Name, Cont, State) ->
  ?YO ("symlink~n", []),
  spawn_link (fun () -> symlink_async (Ctx,
                                       Link,
                                       Inode,
                                       Name, 
                                       Cont,
                                       State) 
              end),
  { noreply, State }.

%% @hidden

write (Ctx, Inode, Data, Offset, Fi, Cont, State) ->
  ?YO ("write~n", []),
  spawn_link (fun () -> write_async (Ctx,
                                     Inode,
                                     Data,
                                     Offset,
                                     Fi,
                                     Cont,
                                     State)
              end),
  { noreply, State }.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

access_async (Ctx, Inode, Mask, Cont, State) ->
  try
    mnesia:activity 
      (State#state.read_meta_context,
       fun () ->
         case mnesia:read (State#state.inode_table, Inode, read) of
           [] ->
             { error, enoent };
           [ #inode{ stat = Stat } ] ->
             case 
               lists:all (fun (X) -> X end,
                          [ case Mode band Mask of
                              0 -> true;
                              _ -> Test (Stat, Ctx)
                            end ||
                            { Mode, Test } 
                               <- [ { ?R_OK, fun is_readable/2 },
                                    { ?W_OK, fun is_writable/2 },
                                    { ?X_OK, fun is_executable/2 } ] ]) of
               true ->
                 ok;
               false ->
                 { error, eacces }
             end
         end
       end,
       [],
       mnesia_frag) of
    ok ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = ok });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

create_async (Ctx, ParentInode, Name, Mode, Fi, Cont, State) ->
  try
    mnesia:activity 
      (State#state.write_meta_context,
       fun () ->
         Param = 
           case do_mknod (Ctx, ParentInode, Name, Mode, { 0, 0 }, State) of
             P = #fuse_entry_param{} ->
               P;
             { error, exist } when Fi#fuse_file_info.flags band ?O_EXCL =:= 0 ->
               #directory_entry{ inode = Ino } = lookup_inode (ParentInode,
                                                               Name,
                                                               State,
                                                               read),
               [ #inode{ stat = Stat } ] = mnesia:read (State#state.inode_table,
                                                        Ino,
                                                        read),
               #fuse_entry_param{ ino = Ino,
                                  generation = 1,
                                  attr = Stat,
                                  attr_timeout_ms = State#state.attr_timeout_ms,
                                  entry_timeout_ms = 
                                    State#state.entry_timeout_ms };
             P ->
               P
           end,

         case Param of
           #fuse_entry_param{ ino = Inode } ->
             case do_open (Inode, Fi, State) of
               #fuse_reply_open{ fuse_file_info = FileInfo } ->
                 #fuse_reply_create{ fuse_entry_param = Param, 
                                     fuse_file_info = FileInfo };
               R -> 
                 R
             end;
           R ->
             R
         end
       end,
       [],
       mnesia_frag) of
    Reply = #fuse_reply_create{} ->
      fuserlsrv:reply (Cont, Reply);
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

%%% NB: After 2^32 inode allocations (on a 32 bit box), we'll be finished.
%%% Of course, real men use 64 bits.

alloc_inode (State) ->
  Inode = 
    mnesia:activity (sync_transaction,
                     fun () ->
                       case mnesia:read (State#state.meta_table, 
                                         next_inode,
                                         write) of
                         [] ->
                           mnesia:write (State#state.meta_table,
                                         #meta{ key = next_inode, value = 2 },
                                         write),
                           2;
                         [ #meta{ value = V } ] ->
                           mnesia:write (State#state.meta_table,
                                         #meta{ key = next_inode, 
                                                value = V + 1 },
                                         write),
                           V + 1
                       end
                     end,
                     [],
                     mnesia_frag),
  Inode.

append_atom (X, Y) when is_atom (X), is_list (Y) ->
  list_to_atom (atom_to_list (X) ++ Y).

delete_blocks (Inode, State) ->
  mnesia:activity 
    (State#state.write_data_context,
     fun () ->
       case mnesia:select (State#state.block_map_table,
                           [ { #block_map{ inode = Inode, block_no = '$1' },
                               [],
                               [ '$1' ] } ],
                           64,
                           write) of
         '$end_of_table' -> 
           ok;
         { BlockNos, Cont } ->
           lists:foreach 
             (fun (B) ->
                mnesia:delete (State#state.block_store_table,
                               { Inode, B },
                               write)
              end,
              BlockNos),
           delete_blocks (Inode, Cont, State)
       end
     end,
     [],
     mnesia_frag).

delete_blocks (Inode, Cont, State) ->
  case mnesia:select (Cont) of
    '$end_of_table' -> 
      mnesia:delete (State#state.block_map_table, Inode, write),
      ok;
    { BlockNos, NewCont } ->
      lists:foreach 
        (fun (B) ->
           mnesia:delete (State#state.block_store_table, { Inode, B }, write)
         end,
         BlockNos),
      delete_blocks (Inode, NewCont, State)
  end.

do_lookup (Parent, Name, State) ->
  try
    mnesia:activity 
      (State#state.read_meta_context,
       fun () -> 
         case lookup_inode (Parent, Name, State, read) of
           #directory_entry{ inode = Inode } ->
             case mnesia:read (State#state.inode_table, Inode, read) of
               [] ->
                 { error, enoent };
               [ #inode{ stat = Stat } ] ->
                 #fuse_entry_param{ ino = Inode,
                                    generation = 1,
                                    attr = Stat,
                                    attr_timeout_ms = 
                                      State#state.attr_timeout_ms,
                                    entry_timeout_ms = 
                                      State#state.entry_timeout_ms }
             end;
           R ->
             R
         end
       end,
       [],
       mnesia_frag) 
  catch
    exit : Y -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ Y ]),
      { aborted, Y }
  end.

% TODO: universal_time ... but not supported in r11b-2 (?)

do_mknod (Ctx, ParentInode, Name, Mode, _Dev, State) ->
  { Mega, Sec, _ } = erlang:now (),
  Now = 1000000 * Mega + Sec,

  try
    mnesia:activity 
      (State#state.write_meta_context,
       fun () ->
         case lookup_inode (ParentInode, Name, State, write) of
           { error, enoent } ->
              Stat = #stat{ st_ino = alloc_inode (State),
                            st_mode = Mode,
                            st_uid = Ctx#fuse_ctx.uid,
                            st_gid = Ctx#fuse_ctx.gid,
                            st_atime = Now,
                            st_mtime = Now,
                            st_ctime = Now },
         
              ok = make_node (Stat, State),
              ok = link_node (ParentInode,
                              Name,
                              Stat#stat.st_ino,
                              State),
         
              #fuse_entry_param{ 
                 ino = Stat#stat.st_ino,
                 generation = 1,
                 attr = Stat#stat{ st_nlink = 1 },
                 attr_timeout_ms = 
                   State#state.attr_timeout_ms,
                 entry_timeout_ms = 
                   State#state.entry_timeout_ms
              };
           _ ->
             { error, eexist }
         end
       end,
       [],
       mnesia_frag) 
  catch 
    exit : Y -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ Y ]),
      { aborted, Y }
  end.

do_open (Inode, Fi, State) ->
  try
    mnesia:activity 
      (State#state.read_meta_context,
       fun () ->
         case mnesia:read (State#state.inode_table, Inode, read) of
           [] ->
             { error, enoent };
           [ #inode{ stat = #stat{ st_mode = Mode } } ] 
             when (Mode band ?S_IFMT =:= ?S_IFDIR) ->
               case (Fi#fuse_file_info.flags band ?O_WRONLY =:= 0) of
                 true ->
                   #fuse_reply_open{ fuse_file_info = Fi };
                 false ->
                   { error, eisdir }
               end;
           [ #inode{ stat = #stat{ st_mode = Mode } } ] 
             when Mode band ?S_IFMT =:= ?S_IFREG ->
               #fuse_reply_open{ fuse_file_info = Fi }
         end
       end,
       [],
       mnesia_frag) 
  catch
    exit : Y -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ Y ]),
      { aborted, Y }
  end.

getattr_async (_Ctx, Inode, Cont, State) ->
  try
    mnesia:activity 
      (State#state.read_meta_context,
       fun () ->
         case mnesia:read (State#state.inode_table, Inode, read) of
           [] ->
             { error, enoent };
           [ #inode{ stat = Stat } ] ->
             Stat
         end
       end,
       [],
       mnesia_frag) of
    R = #stat{} -> 
      fuserlsrv:reply (Cont, 
                       #fuse_reply_attr{ attr = R, 
                                         attr_timeout_ms = 
                                           State#state.attr_timeout_ms });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

getxattr_async (_Ctx, Inode, Name, Size, Cont, State) ->
  try
    mnesia:activity
      (State#state.read_meta_context,
       fun () ->
         case mnesia:read (State#state.xattr_table, { Inode, Name }, read) of
           [] ->
             % hmmm ... man page says ENOATTR ...
             % can't find it in any C system header ... (?)
             { error, enoent };
           [ #xattr{ value = Value } ] -> 
             case Size of 
               0 ->
                 erlang:size (Value);
               N when N < erlang:size (Value) ->
                 { error, erange };
               _ ->
                 Value
             end
         end
       end,
       [],
       mnesia_frag) of
    Count when is_integer (Count) ->
      fuserlsrv:reply (Cont, #fuse_reply_xattr{ count = Count });
    Value when is_binary (Value) ->
      fuserlsrv:reply (Cont, #fuse_reply_buf{ size = erlang:size (Value), 
                                              buf = Value });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

is_empty (Cont) ->
  case mnesia:select (Cont) of
    '$end_of_table' -> true;
    { [], NewCont } -> is_empty (NewCont);
    { _, _ } -> false
  end.

is_empty (Inode, State) ->
  case mnesia:select (State#state.directory_entry_table,
                      [ { #directory_entry{ id = { Inode, '$1' },
                                            _ = '_' },
                          [ { '=/=', '$1', "." },
                            { '=/=', '$1', ".." } ],
                          [ '$1' ] } ],
                      1,
                      read) of
    '$end_of_table' -> true;
    { [], Cont } -> is_empty (Cont);
    { _, _ } -> false
  end.

is_executable (Stat, Ctx) ->
  is_world_executable (Stat#stat.st_mode) orelse
  (Stat#stat.st_gid =:= Ctx#fuse_ctx.gid andalso 
   is_group_executable (Stat#stat.st_mode)) orelse
  (Stat#stat.st_uid =:= Ctx#fuse_ctx.uid andalso
   is_user_executable (Stat#stat.st_mode)).

is_group_executable (Mode) ->
  (Mode band ?S_IXGRP) =/= 0.

is_group_readable (Mode) ->
  (Mode band ?S_IRGRP) =/= 0.

is_group_writable (Mode) ->
  (Mode band ?S_IWGRP) =/= 0.

is_readable (Stat, Ctx) ->
  is_world_readable (Stat#stat.st_mode) orelse
  (Stat#stat.st_gid =:= Ctx#fuse_ctx.gid andalso 
   is_group_readable (Stat#stat.st_mode)) orelse
  (Stat#stat.st_uid =:= Ctx#fuse_ctx.uid andalso
   is_user_readable (Stat#stat.st_mode)).

is_user_executable (Mode) ->
  (Mode band ?S_IXUSR) =/= 0.

is_user_readable (Mode) ->
  (Mode band ?S_IRUSR) =/= 0.

is_user_writable (Mode) ->
  (Mode band ?S_IWUSR) =/= 0.

is_writable (Stat, Ctx) ->
  is_world_writable (Stat#stat.st_mode) orelse
  (Stat#stat.st_gid =:= Ctx#fuse_ctx.gid andalso 
   is_group_writable (Stat#stat.st_mode)) orelse
  (Stat#stat.st_uid =:= Ctx#fuse_ctx.uid andalso
   is_user_writable (Stat#stat.st_mode)).

is_world_executable (Mode) ->
  (Mode band ?S_IXOTH) =/= 0.

is_world_readable (Mode) ->
  (Mode band ?S_IROTH) =/= 0.

is_world_writable (Mode) ->
  (Mode band ?S_IWOTH) =/= 0.

%%% Q: have to check if Inode is a directory, or fuse does that?

link_async (_Ctx, Inode, NewParent, NewName, Cont, State) ->
  try
    mnesia:activity 
      (State#state.write_meta_context,
       fun () ->
         case lookup_inode (NewParent, NewName, State, write) of
           { error, enoent } ->
             ok = link_node (NewParent, NewName, Inode, State),
             do_lookup (NewParent, NewName, State);
           _ ->
             { error, eexist }
         end
       end,
       [],
       mnesia_frag) of
    P = #fuse_entry_param{} ->
      fuserlsrv:reply (Cont, #fuse_reply_entry{ fuse_entry_param = P });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

link_node (ParentInode, Name, Inode, State) ->
  case mnesia:read (State#state.inode_table, Inode, write) of
    [ I = #inode{ stat = Stat = #stat{ st_nlink = NLink } } ] ->
      mnesia:write (State#state.directory_entry_table,
                    #directory_entry{ id = { ParentInode, Name },
                                      inode = Inode },
                    write),
      mnesia:write (State#state.inode_table,
                    I#inode{ stat = Stat#stat{ st_nlink = NLink + 1 } },
                    write),

      case Stat#stat.st_mode band ?S_IFMT of
        ?S_IFDIR ->
          mnesia:write (State#state.directory_entry_table,
                        #directory_entry{ id = { Stat#stat.st_ino, ".." },
                                          inode = ParentInode },
                        write);
        _ ->
          ok
      end
  end.

listxattr_async (_Ctx, Inode, Size, Cont, State) ->
  try
    mnesia:activity
      (State#state.read_meta_context,
       fun () ->
         [ Name || 
           #xattr{ id = { _, Name } } 
             <- mnesia:match_object (State#state.xattr_table,
                                     #xattr{ id = { Inode, '_' }, _ = '_'  },
                                     read) ]
       end,
       [],
       mnesia_frag) of
    Names ->
      Len = lists:foldl (fun (X, Acc) -> Acc + erlang:size (X) end,
                         erlang:length (Names),
                         Names),

      case Size of 
        0 ->
          fuserlsrv:reply (Cont, #fuse_reply_xattr{ count = Len });
        N when N < Len ->
          fuserlsrv:reply (Cont, #fuse_reply_err{ err = erange });
        _ ->
          fuserlsrv:reply (Cont, #fuse_reply_buf{ size = Len,
                                                  buf = [ [ X, <<0:8>> ]
                                                          || X <- Names ] })
      end
  catch
    exit : X -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

lookup_async (_Ctx, Parent, Name, Cont, State) ->
  case do_lookup (Parent, Name, State) of
    Param = #fuse_entry_param{} ->
      fuserlsrv:reply (Cont, #fuse_reply_entry{ fuse_entry_param = Param });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

lookup_inode (ParentInode, Name, State, LockKind) ->
  case mnesia:read (State#state.directory_entry_table,
                    { ParentInode, Name },
                    LockKind) of
    [] ->
      { error, enoent };
    [ E ] ->
      E
  end.

make_node (Stat, State) ->
  mnesia:write (State#state.inode_table, 
                #inode{ id = Stat#stat.st_ino, stat = Stat },
                write),

  case Stat#stat.st_mode band ?S_IFMT of
    ?S_IFDIR ->
      mnesia:write (State#state.directory_entry_table,
                    #directory_entry{ id = { Stat#stat.st_ino, "." },
                                      inode = Stat#stat.st_ino },
                    write);
    _ ->
      ok
  end.

max (A, B) when A > B -> A;
max (_, B) -> B.

min (A, B) when A < B -> A;
min (_, B) -> B.

mknod_async (Ctx, ParentInode, Name, Mode, Dev, Cont, State) ->
  case do_mknod (Ctx, ParentInode, Name, Mode, Dev, State) of
    Param = #fuse_entry_param{} ->
      fuserlsrv:reply (Cont, #fuse_reply_entry{ fuse_entry_param = Param });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

open_async (_Ctx, Inode, Fi, Cont, State) ->
  case do_open (Inode, Fi, State) of
    Open = #fuse_reply_open{} ->
      fuserlsrv:reply (Cont, Open);
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

read_async (_Ctx, Inode, Size, Offset, _Fi, Cont, State) ->
  try
    mnesia:activity 
      (State#state.read_data_context,
       fun () ->
         case mnesia:read (State#state.inode_table, Inode, read) of
           [] ->
             { error, enoent };
           [ #inode{ stat = #stat{ st_size = FileSize } } ] ->
             BlockSize = State#state.block_size_bytes,
    
             FirstBlockNo = Offset div BlockSize,
             FirstBlockOffset = Offset - BlockSize * FirstBlockNo,
        
             { ok,
               min (FileSize, Size),
               read_blocks (Inode, 
                          FirstBlockNo, 
                          FirstBlockOffset,
                          BlockSize,
                          min (FileSize, Size),
                          State,
                          []) }
         end
       end,
       [],
       mnesia_frag) of
    { ok, Count, IoList } ->
      fuserlsrv:reply (Cont, #fuse_reply_buf{ size = Count, buf = IoList })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

read_blocks (Inode,
             BlockNo,
             BlockOffset,
             BlockSize,
             Remaining,
             State,
             Acc) when Remaining > 0 ->
  ReadSize = min (BlockSize - BlockOffset, Remaining),

  Read = 
    case mnesia:read (State#state.block_store_table, 
                      { Inode, BlockNo },
                      read) of
      [] ->
        <<0:(8 * ReadSize)>>;
      [ #block_store{ data = <<_:BlockOffset/binary, 
                                 Data:ReadSize/binary,
                                 _/binary>> } ] ->
        Data
    end,

  read_blocks (Inode,
               BlockNo + 1,
               0,
               BlockSize,
               Remaining - ReadSize,
               State,
               [ Read | Acc ]);
read_blocks (_, _, _, _, _, _, Acc) -> 
  lists:reverse (Acc).

readdir_async (_Ctx, Inode, Size, Offset, _Fi, Cont, State) ->
  try
    mnesia:activity
      (State#state.read_meta_context,
       fun () ->
         case mnesia:select (State#state.directory_entry_table,
                             [ { #directory_entry{ id = { Inode, '_' },
                                                   inode = '_' },
                               [],
                               [ '$_' ] } ],
                             64,
                             read) of
           { Objects, More } when length (Objects) < Offset ->
             readdir_entries (More, Size, length (Objects), Offset, State, []);
           { Objects, More } when length (Objects) >= Offset ->
             { Action, { RevElements, { _, CurSize, Size } } } = 
               readdir_add_entries (lists:nthtail (Offset, Objects),
                                    Size,
                                    0,
                                    [],
                                    State),

             case Action of
               stop ->
                 lists:reverse (RevElements);
               continue ->
                 readdir_entries (More, 
                                  Size - CurSize, 
                                  length (Objects) - Offset,
                                  Offset,
                                  State,
                                  RevElements)
             end
         end
       end,
       [],
       mnesia_frag) of
    Entries when is_list (Entries) ->
      fuserlsrv:reply (Cont,
                       #fuse_reply_direntrylist{ direntrylist = Entries });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

readdir_add_entries (NewElements, Size, InitOffset, Elements, State) ->
  take_while (fun (#directory_entry{ id = { _, Name }, inode = Inode },
                   { Offset, Total, Max }) ->
                E = 
                  case mnesia:read (State#state.inode_table, Inode, read) of
                    [] ->
                      #direntry{ name = "__inode_" ++ integer_to_list (Inode),
                                 offset = Offset + 1,
                                 stat = #stat{} };
                    [ #inode{ stat = S } ] ->
                      #direntry{ name = Name, offset = Offset + 1, stat = S }
                  end,
    
                Cur = fuserlsrv:dirent_size (E),
                if
                  Total + Cur =< Max ->
                    { continue, E, { Offset + 1, Total + Cur, Max } };
                  true ->
                    stop
                end
              end,
              { Elements, { InitOffset, 0, Size } },
              NewElements).

readdir_entries (Cont, Size, CurOffset, Offset, State, Acc) ->
  case mnesia:select (Cont) of
    '$end_of_table' ->
      lists:reverse (Acc);
    { Objects, NewCont } when CurOffset + length (Objects) < Offset ->
      readdir_entries (NewCont, 
                       Size,
                       CurOffset + length (Objects),
                       Offset,
                       State,
                       Acc);
    { Objects, NewCont } when CurOffset + length (Objects) >= Offset ->
      { Action, { RevElements, { _, CurSize, Size } } } = 
        readdir_add_entries (lists:nthtail (max (Offset - CurOffset, 0),
                                            Objects), 
                             Size, 
                             CurOffset,
                             Acc,
                             State),

      case Action of
        stop ->
          lists:reverse (RevElements);
        continue ->
          readdir_entries (NewCont, 
                           Size - CurSize, 
                           CurOffset 
                           + length (Objects) 
                           - max (Offset - CurOffset, 0),
                           Offset,
                           State,
                           RevElements)
      end
  end.

readlink_async (_Ctx, Inode, Cont, State) ->
  try
    mnesia:activity 
      (State#state.read_meta_context,
        fun () ->
          case mnesia:read (State#state.symlink_table, Inode, read) of
            [] ->
              { error, enoent };
            [ S = #symlink{} ] ->
              S
          end
        end,
        [],
        mnesia_frag) of
    #symlink{ link = Link } ->
      fuserlsrv:reply (Cont, #fuse_reply_readlink{ link = Link });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

removexattr_async (_Ctx, Inode, Name, Cont, State) ->
  try
    mnesia:activity
      (State#state.write_meta_context,
       fun () ->
         case mnesia:read (State#state.xattr_table, { Inode, Name }, write) of
           [] ->
             { error, enoent };
           [ #xattr{} ] ->
             mnesia:delete (State#state.xattr_table, { Inode, Name }, write)
         end
       end,
       [],
       mnesia_frag) of
    ok ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = ok });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

%%% Q: need to check if { NewParent, NewName } exists?

rename_async (_Ctx, Parent, Name, NewParent, NewName, Cont, State) ->
  try
    mnesia:activity 
      (State#state.write_meta_context,
       fun () ->
         case lookup_inode (Parent, Name, State, write) of
           #directory_entry{ inode = Inode } ->
             ok = link_node (NewParent, NewName, Inode, State),
             ok = unlink_node (Parent, Name, State);
           R ->
             R
         end
       end,
       [],
       mnesia_frag) of
    ok ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = ok });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

rmdir_async (_Ctx, ParentInode, Name, Cont, State) ->
  try
    mnesia:activity
      (State#state.write_meta_context,
       fun () -> 
         case lookup_inode (ParentInode, Name, State, write) of
           #directory_entry{ inode = Inode } ->
             case is_empty (Inode, State) of
               false ->
                 { error, enotempty };
               true ->
                 unlink_node (ParentInode, Name, State)
             end;
           R ->
             R
         end
       end,
       [],
       mnesia_frag) of
    ok ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = ok });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

setxattr_async (_Ctx, Inode, Name, Value, Flags, Cont, State) ->
  try
    mnesia:activity
      (State#state.write_meta_context,
       fun () ->
         case mnesia:read (State#state.xattr_table, { Inode, Name }, write) of
           [] when Flags band ?XATTR_REPLACE =/= 0 ->
             { error, enoent };
           [] ->
             mnesia:write (State#state.xattr_table,
                           #xattr{ id = { Inode, Name },
                                   value = Value },
                           write);
           [ #xattr{} ] when Flags band ?XATTR_CREATE =/= 0 ->
             { error, eexist };
           [ #xattr{} ] ->
             mnesia:write (State#state.xattr_table,
                           #xattr{ id = { Inode, Name },
                                   value = Value },
                           write)
         end
       end,
       [],
       mnesia_frag) of
    ok ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = ok });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

unlink_async (_Ctx, Parent, Name, Cont, State) ->
  try
    mnesia:activity 
      (State#state.write_meta_context,
       fun () -> unlink_node (Parent, Name, State) end,
       [],
       mnesia_frag) of
    ok ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = ok });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X -> 
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

unlink_node (ParentInode, Name, State) ->
  case lookup_inode (ParentInode, Name, State, write) of
    #directory_entry{ inode = Inode } ->
      mnesia:delete (State#state.directory_entry_table, 
                     { ParentInode, Name },
                     write),
      case mnesia:read (State#state.inode_table, Inode, write) of
        [ I = #inode{ stat = Stat = #stat{ st_nlink = NLink } } ] 
          when NLink > 1 ->
          mnesia:write (State#state.inode_table,
                        I#inode{ stat = Stat#stat{ st_nlink = NLink - 1 } },
                        write);
        [ #inode{ stat = #stat{ st_mode = Mode } } ] ->
          case Mode band ?S_IFMT of
            ?S_IFDIR -> ok;     % supposed to be empty at this point
            ?S_IFLNK -> mnesia:delete (State#state.symlink_table, Inode, write);
            ?S_IFREG -> delete_blocks (Inode, State)
          end,
          mnesia:delete (State#state.inode_table, Inode, write);
        [] ->
          ok
      end;
    R ->
      R
  end.

setattr_stat (Stat, Attr, ToSet) ->
  NewMode = 
    if ToSet band ?FUSE_SET_ATTR_MODE > 0 -> Attr#stat.st_mode;
       true -> Stat#stat.st_mode
    end,

  NewUid = 
    if ToSet band ?FUSE_SET_ATTR_UID > 0 -> Attr#stat.st_uid;
       true -> Stat#stat.st_uid
    end,

  NewGid = 
    if ToSet band ?FUSE_SET_ATTR_GID > 0 -> Attr#stat.st_gid;
       true -> Stat#stat.st_gid
    end,

  NewSize = 
    if ToSet band ?FUSE_SET_ATTR_SIZE > 0 -> Attr#stat.st_size;
       true -> Stat#stat.st_size
    end,

  NewATime = 
    if ToSet band ?FUSE_SET_ATTR_ATIME > 0 -> Attr#stat.st_atime;
       true -> Stat#stat.st_atime
    end,

  NewMTime = 
    if ToSet band ?FUSE_SET_ATTR_MTIME > 0 -> Attr#stat.st_mtime;
       true -> Stat#stat.st_mtime
    end,

  Stat#stat{ st_mode = NewMode,
             st_uid = NewUid,
             st_gid = NewGid,
             st_size = NewSize,
             st_atime = NewATime,
             st_mtime = NewMTime }.

setattr_async (_Ctx, Inode, Attr, ToSet, _Fi, Cont, State) ->
  try
    mnesia:activity 
      (State#state.write_meta_context,
       fun () ->
         case mnesia:read (State#state.inode_table, Inode, write) of
           [] ->
             { error, enoent };
           [ I = #inode{ stat = Stat } ] ->
             NewStat = setattr_stat (Stat, Attr, ToSet),
             mnesia:write (State#state.inode_table,
                           I#inode{ stat = NewStat },
                           write),
             NewStat
         end
       end,
       [],
       mnesia_frag) of
    Stat = #stat{} ->
      fuserlsrv:reply (Cont, 
                       #fuse_reply_attr{ attr = Stat,
                                         attr_timeout_ms = 
                                           State#state.attr_timeout_ms });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

% cheesy calc based upon 2 gig fragments and uniform usage ... 
% better than nothing (?)

statfs_async (_Ctx, _Inode, Cont, State) ->
  try
    { value, { n_fragments, N } } = 
      lists:keysearch (n_fragments,
                       1,
                       mnesia:table_info (State#state.inode_table, 
                                          frag_properties)),

    Bytes = mnesia:table_info (State#state.inode_table, memory),

    MaxInode = 
      case mnesia:activity (State#state.read_meta_context,
                            fun () ->
                              mnesia:read (State#state.meta_table,
                                           next_inode,
                                           read)
                            end,
                            [],
                            mnesia_frag) of
        [] -> 1;
        [ #meta{ value = V } ] -> V
      end,

    #statvfs{ f_bsize = State#state.block_size_bytes,
              f_frsize = State#state.block_size_bytes,
              f_blocks = (N * (1 bsl 31)) div State#state.block_size_bytes,
              f_bfree = (N * ((1 bsl 31) - Bytes)) div State#state.block_size_bytes,
              f_bavail = (N * ((1 bsl 31) - Bytes)) div State#state.block_size_bytes,
              f_files = 1 bsl 32,
              f_ffree = 1 bsl 32 - MaxInode,
              f_favail = 1 bsl 32 - MaxInode,
              f_fsid = 36#n54,
              f_flag = 0,
              f_namemax = 36#sup } of
    S = #statvfs{} ->
      fuserlsrv:reply (Cont, #fuse_reply_statfs{ statvfs = S })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

symlink_async (Ctx, Link, Inode, Name, Cont, State) ->
  try
    mnesia:activity 
      (State#state.write_meta_context,
       fun () ->
         case do_mknod (Ctx, Inode, Name, ?S_IFLNK, { 0, 0 }, State) of
           Param = #fuse_entry_param{ ino = PInode } ->
             mnesia:write (State#state.symlink_table,
                           #symlink{ inode = PInode, link = Link },
                           write),
             Param;
           R -> 
             R
         end
       end,
       [],
       mnesia_frag) of
    Param = #fuse_entry_param{} ->
      fuserlsrv:reply (Cont, #fuse_reply_entry{ fuse_entry_param = Param });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

take_while (_Fun, { List0, Acc0 }, []) ->
  { continue, { List0, Acc0 } };
take_while (Fun, { List0, Acc0 }, [ H | T ]) ->
  case Fun (H, Acc0) of
    stop ->
      { stop, { List0, Acc0 } };
    { continue, Elem, NewAcc } ->
      take_while (Fun, { [ Elem | List0 ], NewAcc }, T)
  end.

write_async (_Ctx, Inode, Data, Offset, _Fi, Cont, State) ->
  try
    mnesia:activity
      (State#state.write_data_context,
        fun () ->
          BlockSize = State#state.block_size_bytes,
          DataSize = erlang:size (Data),

          case
            mnesia:activity 
              (State#state.write_meta_context,
               fun () ->
                 case mnesia:read (State#state.inode_table, Inode, write) of
                   [] ->
                     { error, enoent };
                   [ I = #inode{ stat = Stat } ] ->
                     NewMinBlocks = (DataSize + Offset) div BlockSize,
                     NewMinSize = DataSize + Offset,

                     mnesia:write 
                       (State#state.inode_table, 
                        I#inode{ 
                          stat = 
                            Stat#stat{ st_blocks = max (NewMinBlocks,
                                                        Stat#stat.st_blocks),
                                       st_size = max (NewMinSize,
                                                      Stat#stat.st_size) } },
                        write)
                 end
               end,
               [],
               mnesia_frag) of
            ok ->
              FirstBlockNo = Offset div BlockSize,
              FirstBlockOffset = Offset - BlockSize * FirstBlockNo,
    
              { ok, write_blocks (Inode, 
                                  FirstBlockNo, 
                                  FirstBlockOffset,
                                  BlockSize,
                                  0,
                                  Data,
                                  State) };
            R ->
              R
          end
        end,
        [],
        mnesia_frag) of
    { ok, Count } when Count =:= erlang:size (Data) ->
      fuserlsrv:reply (Cont, #fuse_reply_write{ count = Count });
    { error, Reason } ->
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = Reason });
    X ->
      ?walkenfs_error_msg ("unexpected result ~p ~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  catch
    exit : X ->
      ?walkenfs_error_msg ("caught exit : ~p~n", [ X ]),
      fuserlsrv:reply (Cont, #fuse_reply_err{ err = eio })
  end.

write_blocks (Inode, BlockNo, BlockOffset, BlockSize, Offset, Data, State)
  when erlang:size (Data) > Offset ->
  WriteSize = min (BlockSize - BlockOffset, erlang:size (Data) - Offset),
  <<_:Offset/binary, Bytes:WriteSize/binary, _/binary>> = Data,
  Rem = BlockSize - BlockOffset - WriteSize,

  Block = 
    case mnesia:read (State#state.block_store_table,
                      { Inode, BlockNo },
                      write) of
      [] ->
        mnesia:write (State#state.block_map_table,
                      #block_map{ inode = Inode, block_no = BlockNo },
                      write),
        <<0:(8 * BlockOffset), Bytes:WriteSize/binary, 0:(8 * Rem)>>;
      [ #block_store{ data = <<Pre:BlockOffset/binary, 
                               _:WriteSize/binary,
                               Post:Rem/binary>> } ] ->
        <<Pre:BlockOffset/binary, Bytes:WriteSize/binary, Post:Rem/binary>>
    end,

  mnesia:write (State#state.block_store_table,
                #block_store{ id = { Inode, BlockNo }, data = Block },
                write),

  write_blocks (Inode,
                BlockNo + 1,
                0,
                BlockSize,
                Offset + WriteSize,
                Data,
                State);
write_blocks (_, _, _, _, DataOffset, _, _) ->
  DataOffset.

-ifdef (EUNIT).

setup () ->
  Dir = "walkentmp." ++ os:getpid (),
  os:cmd ("rm -rf Mnesia*"),
  file:make_dir (Dir),
  mnesia:start (),
  mnesia:change_table_copy_type (schema, node (), disc_copies),
  { ok, [ App ] } = file:consult ("../src/walkenfs.app"),
  application:load (App),
  application:set_env (walkenfs, mount_point, Dir),
  application:set_env (walkenfs, init_fragments, 3),
  application:set_env (walkenfs, init_copies, 1),
  application:set_env (walkenfs, copy_type, n_ram_copies),
  walkenfs:start (),
  Dir.

cleanup (Dir) ->
  walkenfs:stop (),
  mnesia:stop (),
  file:del_dir (Dir),
  os:cmd ("rm -rf Mnesia*").

%-=====================================================================-
%-                                Tests                                -
%-=====================================================================-

% TODO: i get deadlock when i try to run these tests without creating
% a slave node.  this despite using the async thread pool.  why?

% TODO: things that cannot be tested from the file driver
%
%  1. opening a directory O_WRONLY or O_RDWR
%    1a. it's an illegal operation, but would be nice to get coverage
%  2. opening a file with O_EXCL
%  3. statfs (ok, testing via df)
%  4. extended attributes (wrote C helper program)
%  5. posix locking

attr_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch ({ ok, _IoDevice },
                    rpc:call (Other,
                              file,
                              open,
                              [ Dir ++ "/flass",
                                [ raw, write ] ])),

      ?assertMatch ({ ok, #file_info{} },
                    rpc:call (Other,
                              file,
                              read_file_info,
                              [ Dir ++ "/flass" ])),

      ?assertMatch (ok,
                    rpc:call (Other,
                              file,
                              write_file_info,
                              [ Dir ++ "/flass",
                                #file_info{ mode = 69 } ])),

      ?assertMatch ({ ok, #file_info{ mode = 69 bor ?S_IFREG } },
                    rpc:call (Other,
                              file,
                              read_file_info,
                              [ Dir ++ "/flass" ])),

      ?assertMatch (ok,
                    rpc:call (Other,
                              file,
                              write_file_info,
                              [ Dir ++ "/flass",
                                #file_info{ mode = 45 } ])),

      ?assertMatch ({ ok, #file_info{ mode = 45 bor ?S_IFREG } },
                    rpc:call (Other,
                              file,
                              read_file_info,
                              [ Dir ++ "/flass" ])),
      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "", F }
  }.

link_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch ({ ok, _IoDevice },
                    rpc:call (Other,
                              file,
                              open,
                              [ Dir ++ "/flass",
                                [ raw, write ] ])),

      ?assertMatch (ok,
                    rpc:call (Other,
                              file,
                              make_link,
                              [ Dir ++ "/flass",
                                Dir ++ "/turg" ])),

      ?assert (rpc:call (Other, file, read_file_info, [ Dir ++ "/flass" ]) =:=
               rpc:call (Other, file, read_file_info, [ Dir ++ "/turg" ])),

      ?assertMatch ({ ok, #file_info{ links = 2 } },
                    rpc:call (Other, file, read_file_info, [ Dir ++ "/flass" ])),

      ?assertMatch ({ ok, #file_info{ links = 2 } },
                    rpc:call (Other, file, read_file_info, [ Dir ++ "/turg" ])),

      ?assertMatch ({ error, eexist },
                    rpc:call (Other,
                              file,
                              make_link,
                              [ Dir ++ "/flass",
                                Dir ++ "/turg" ])),

      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "", F }
  }.

mkdir_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch ({ ok, [] }, 
                    rpc:call (Other, file, list_dir, [ Dir ])),
      ?assertMatch (ok, 
                    rpc:call (Other, file, make_dir, [ Dir ++ "/flass" ])),
      ?assertMatch ({ error, eexist },
                    rpc:call (Other, file, make_dir, [ Dir ++ "/flass" ])),
      ?assertMatch ({ ok, [ "flass" ] }, 
                    rpc:call (Other, file, list_dir, [ Dir ])),
      ?assertMatch ({ error, eisdir },
                    rpc:call (Other, file, open, [ Dir, [ write, raw, binary ] ])),
      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "", F }
  }.

% ok, don't want to depend upon quickcheck, so here's some cheese

-define (FORALL (Var, Gen, Cond), fun (A) -> Var = (Gen) (A), Cond end).

flasscheck (N, Limit, P) -> flasscheck (1, N, math:log (Limit), P).

flasscheck (M, N, LogLimit, P) when M =< N -> 
  Size = trunc (math:exp (LogLimit * M / N)),
  true = P (Size),
  io:format (".", []),
  flasscheck (M + 1, N, LogLimit, P);
flasscheck (_, N, _, _) -> 
  io:format ("~n~p tests passed~n", [ N ]),
  ok.

pread_write_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      T = ?FORALL (X,
                   fun (N) -> { random:uniform (N),
                                random:uniform (N) }
                   end,
                   (fun ({ Offset, Size }) ->
                      ?assertMatch (ok,
                                    rpc:call (Other,
                                              erlang,
                                              apply,
                                              [ fun pwriteread/3,
                                                [ Dir ++ "/flass",
                                                  Offset,
                                                  Size ] ])),
                      true
                    end) (X)),

      ok = flasscheck (1000, 10000, T),
      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "-pa ../src", { timeout, 60, F } }
  }.

pwriteread (File, Offset, Size) ->
  { ok, IoDevice } = file:open (File, [ raw, binary, read, write ]),

  Bytes = list_to_binary ([ random:uniform (256) - 1 
                            || _ <- lists:seq (1, Size) ]),

  ok = file:pwrite (IoDevice, [ { Offset, Bytes } ]),

  { ok, [ Bytes ] } = file:pread (IoDevice, [ { Offset, Size } ]),

  ok = file:close (IoDevice).

read_write_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch ({ error, enoent },
                    rpc:call (Other,
                              file,
                              read_file,
                              [ Dir ++ "/flass" ])),
      ?assertMatch (ok,
                    rpc:call (Other, 
                              file,
                              write_file,
                              [ Dir ++ "/flass",
                                <<"dild">> ])),
      ?assertMatch ({ ok, <<"dild">> },
                    rpc:call (Other,
                              file,
                              read_file,
                              [ Dir ++ "/flass" ])),
      ?assertMatch ({ ok, [ "flass" ] }, 
                    rpc:call (Other, file, list_dir, [ Dir ])),
      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "", F }
  }.

rename_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch ({ ok, _IoDevice },
                    rpc:call (Other,
                              file,
                              open,
                              [ Dir ++ "/flass",
                                [ raw, write ] ])),

      { ok, FileInfo } = rpc:call (Other,
                                   file,
                                   read_file_info,
                                   [ Dir ++ "/flass" ]),

      ?assertMatch (ok,
                    rpc:call (Other, file, rename, [ Dir ++ "/flass",
                                                     Dir ++ "/turg" ])),

      ?assertMatch ({ error, enoent },
                    rpc:call (Other, file, read_file_info, [ Dir ++ "/flass" ])),

      ?assertMatch ({ ok, FileInfo },
                    rpc:call (Other, file, read_file_info, [ Dir ++ "/turg" ])),

      ?assertMatch ({ error, enoent },
                    rpc:call (Other, file, rename, [ Dir ++ "/flass",
                                                     Dir ++ "/turg" ])),

      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "", F }
  }.

rmdir_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch ({ ok, [] }, 
                    rpc:call (Other, file, list_dir, [ Dir ])),
      ?assertMatch (ok, 
                    rpc:call (Other, file, make_dir, [ Dir ++ "/flass" ])),
      ?assertMatch ({ ok, [ "flass" ] }, 
                    rpc:call (Other, file, list_dir, [ Dir ])),
      ?assertMatch (ok, 
                    rpc:call (Other, file, make_dir, [ Dir ++ "/flass/turg" ])),

      % The rmdir() and rename() functions originated in 4.2 BSD,
      % and they used [ENOTEMPTY] for the condition when the directory
      % to be removed does not exist or new already exists. When the
      % 1984 /usr/group standard was published, it contained [EEXIST]
      % instead. When these functions were adopted into System V, the
      % 1984 /usr/group standard was used as a reference. Therefore,
      % several existing applications and implementations support/use
      % both forms, and no agreement could be reached on either
      % value. 
      % http://www.opengroup.org/onlinepubs/009695399/functions/rmdir.html

      ?assertMatch ({ error, Error } when (Error =:= enotempty) or (Error =:= eexist),
                    rpc:call (Other, file, del_dir, [ Dir ++ "/flass" ])),
      ?assertMatch (ok, 
                    rpc:call (Other, file, del_dir, [ Dir ++ "/flass/turg" ])),
      ?assertMatch (ok, 
                    rpc:call (Other, file, del_dir, [ Dir ++ "/flass" ])),
      ?assertMatch ({ error, enoent },
                    rpc:call (Other, file, del_dir, [ Dir ++ "/flass" ])),
      ?assertMatch ({ ok, [] }, 
                    rpc:call (Other, file, list_dir, [ Dir ])),
      ?assertMatch ({ error, ebusy },
                    rpc:call (Other, file, del_dir, [ Dir ])),
      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "", F }
  }.

statfs_helper (Dir) ->
  BytesUsed = 
    lists:nth 
      (3,
       string:tokens 
         (lists:last (string:tokens (os:cmd ("df " ++ Dir), "\n")), " \t")),

  BytesAvail = 
    lists:nth 
      (4,
       string:tokens 
         (lists:last (string:tokens (os:cmd ("df " ++ Dir), "\n")), " \t")),

  IUsed = 
    lists:nth 
      (3,
       string:tokens 
         (lists:last (string:tokens (os:cmd ("df -i " ++ Dir), "\n")),
          " \t")),

  IAvail = 
    lists:nth 
      (4,
       string:tokens 
         (lists:last (string:tokens (os:cmd ("df -i " ++ Dir), "\n")),
          " \t")),

  { BytesUsed, BytesAvail, IUsed, IAvail }.

statfs_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      { BytesUsed, BytesAvail, IUsed, IAvail } = 
        rpc:call (Other, erlang, apply, [ fun statfs_helper/1, [ Dir ] ]),

      os:cmd ("mkdir " ++ Dir ++ "/flass"),

      { NewBytesUsed, NewBytesAvail, NewIUsed, NewIAvail } = 
        rpc:call (Other, erlang, apply, [ fun statfs_helper/1, [ Dir ] ]),

      ?assert (BytesUsed =< NewBytesUsed),
      ?assert (BytesAvail >= NewBytesAvail),
      ?assert (IUsed < NewIUsed),
      ?assert (IAvail > NewIAvail),

      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "-pa ../src", F }
  }.

symlink_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch ({ ok, _IoDevice },
                    rpc:call (Other,
                              file,
                              open,
                              [ Dir ++ "/flass",
                                [ raw, write ] ])),

      ?assertMatch (ok,
                    rpc:call (Other,
                              file,
                              make_symlink,
                              [ "flass",
                                Dir ++ "/turg" ])),

      ?assert (rpc:call (Other, file, read_file_info, [ Dir ++ "/flass" ]) =:=
               rpc:call (Other, file, read_file_info, [ Dir ++ "/turg" ])),

      ?assertMatch ({ ok, "flass" },
                    rpc:call (Other, file, read_link, [ Dir ++ "/turg" ])),

      ?assertMatch ({ ok, #file_info{ type = symlink } },
                    rpc:call (Other, file, read_link_info, [ Dir ++ "/turg" ])),

      ?assertMatch ({ error, eexist },
                    rpc:call (Other,
                              file,
                              make_symlink,
                              [ Dir ++ "/flass",
                                Dir ++ "/turg" ])),

      ?assertMatch (ok,
                    rpc:call (Other, file, delete, [ Dir ++ "/turg" ])),

      ?assertMatch ({ error, enoent },
                    rpc:call (Other, file, read_link_info, [ Dir ++ "/turg" ])),

      ?assertMatch ({ error, enoent },
                    rpc:call (Other, file, read_link, [ Dir ++ "/turg" ])),

      ?assertMatch (ok,
                    rpc:call (Other,
                              file,
                              make_symlink,
                              [ "dild",
                                Dir ++ "/turg" ])),

      ?assertMatch ({ error, eexist },
                    rpc:call (Other,
                              file,
                              make_symlink,
                              [ Dir ++ "/flass",
                                Dir ++ "/turg" ])),

      ?assertMatch ({ ok, "dild" },
                    rpc:call (Other, file, read_link, [ Dir ++ "/turg" ])),

      ?assertMatch ({ ok, #file_info{ type = symlink } },
                    rpc:call (Other, file, read_link_info, [ Dir ++ "/turg" ])),


      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "", F }
  }.

unlink_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch ({ ok, _IoDevice },
                    rpc:call (Other,
                              file,
                              open,
                              [ Dir ++ "/flass",
                                [ raw, write ] ])),

      ?assertMatch ({ ok, #file_info{} },
                    rpc:call (Other, file, read_file_info, [ Dir ++ "/flass" ])),

      ?assertMatch (ok,
                    rpc:call (Other, file, delete, [ Dir ++ "/flass" ])),

      ?assertMatch ({ error, enoent },
                    rpc:call (Other, file, read_file_info, [ Dir ++ "/flass" ])),

      ?assertMatch ({ error, enoent },
                    rpc:call (Other, file, delete, [ Dir ++ "/flass" ])),

      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "", F }
  }.

xattr_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),

  F =
    fun () ->
      "-1" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr set " ++ Dir ++ 
                                          "/flass turg wazzup none"),
                                  " ")),
      os:cmd ("touch " ++ Dir ++ "/flass"),

      "-1" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr get " ++ Dir ++ 
                                          "/flass turg 0"),
                                  " ")),

      "-1" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr set " ++ Dir ++ 
                                          "/flass turg wazzup replace"),
                                  " ")),

      "0" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr set " ++ Dir ++ 
                                          "/flass turg wazzup none"),
                                  " ")),

      "-1" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr set " ++ Dir ++ 
                                          "/flass turg wazzup create"),
                                  " ")),

      "6" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr get " ++ Dir ++ 
                                          "/flass turg 0"),
                                  " ")),

      "-1" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr get " ++ Dir ++ 
                                          "/flass turg 4"),
                                  " ")),

      "6" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr get " ++ Dir ++ 
                                          "/flass turg 8"),
                                  " ")),

      "wazzup\n" = 
        lists:nth (3, 
                   string:tokens (os:cmd ("../tests/xattr get " ++ Dir ++ 
                                          "/flass turg 8"),
                                  " ")),

      "0" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr set " ++ Dir ++ 
                                          "/flass turg mega none"),
                                  " ")),

      "mega\n" = 
        lists:nth (3, 
                   string:tokens (os:cmd ("../tests/xattr get " ++ Dir ++ 
                                          "/flass turg 8"),
                                  " ")),

      "-1" = 
        lists:nth (2,
                   string:tokens (os:cmd ("../tests/xattr list " ++ Dir ++
                                          "/flass 1"),
                                  " ")),

      "5" = 
        lists:nth (2,
                   string:tokens (os:cmd ("../tests/xattr list " ++ Dir ++
                                          "/flass 10"),
                                  " ")),

      "turg\0\n" = 
        lists:nth (3,
                   string:tokens (os:cmd ("../tests/xattr list " ++ Dir ++
                                          "/flass 10"),
                                  " ")),

      "0" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr set " ++ Dir ++ 
                                          "/flass warez mega none"),
                                  " ")),

      "11" = 
        lists:nth (2,
                   string:tokens (os:cmd ("../tests/xattr list " ++ Dir ++
                                          "/flass 11"),
                                  " ")),

      "turg\0warez\0\n" = 
        lists:nth (3,
                   string:tokens (os:cmd ("../tests/xattr list " ++ Dir ++
                                          "/flass 11"),
                                  " ")),

      "0" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr remove " ++ Dir ++ 
                                          "/flass warez"),
                                  " ")),

      "-1" = 
        lists:nth (2, 
                   string:tokens (os:cmd ("../tests/xattr remove " ++ Dir ++ 
                                          "/flass warez"),
                                  " ")),

      "turg\0\n" = 
        lists:nth (3,
                   string:tokens (os:cmd ("../tests/xattr list " ++ Dir ++
                                          "/flass 10"),
                                  " ")),
      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    F
  }.

zerofill (File) ->
  file:delete (File),

  { ok, IoDevice } = file:open (File, [ raw, binary, read, write ]),

  ok = file:pwrite (IoDevice, [ { 1024, <<1>> } ]),

  { ok, [ <<0>> ] } = file:pread (IoDevice, [ { 513, 1 } ]),

  ok = file:close (IoDevice).

zerofill_test_ () ->
  Dir = "walkentmp." ++ os:getpid (),
  [ Host ] = tl (string:tokens (atom_to_list (node ()), "@")),
  Other = list_to_atom ("flass@" ++ Host),

  F = 
    fun () ->
      ?assertMatch (ok,
                    rpc:call (Other,
                              erlang,
                              apply,
                              [ fun zerofill/1,
                                [ Dir ++ "/flass" ] ])),
      true
    end,

  { setup,
    fun setup/0,
    fun cleanup/1,
    { node, Other, "-pa ../src", F }
  }.

-endif.
