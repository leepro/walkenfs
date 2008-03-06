-module (walkenfsfragmentsrv).
-export ([ start_link/6 ]).
-behaviour (gen_fragment).
-export ([ init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3]).
%-behaviour (mnesia_frag_hash).
-export ([ init_state/2,
           add_frag/1,
           del_frag/1,
           key_to_frag_number/2,
           match_spec_to_frag_numbers/2 ]).

-include_lib ("fuserl/include/fuserl.hrl").
-include ("walkenfs.hrl").

-define (is_bool (X), (((X) =:= true) or ((X) =:= false))).
-define (is_timeout (X), ((is_integer (X) andalso X > 0) orelse 
                          (X) =:= infinity)).
-define (is_copy_type (X), (((X) =:= n_ram_copies) orelse
                            ((X) =:= n_disc_copies) orelse
                            ((X) =:= n_disc_only_copies))).

-record (state, { }).

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link (ReuseNode,
            StartTimeout,
            InitFragments,
            InitCopies,
            CopyType,
            Prefix) when ?is_bool (ReuseNode),
                         ?is_timeout (StartTimeout),
                         is_integer (InitFragments), InitFragments > 0,
                         is_integer (InitCopies), InitCopies > 0,
                         ?is_copy_type (CopyType),
                         is_atom (Prefix) ->
  gen_fragment:start_link ({ local, ?MODULE },
                           ?MODULE,
                           ReuseNode,
                           [ InitFragments, InitCopies, CopyType, Prefix ],
                           [ { timeout, StartTimeout } ]).

%-=====================================================================-
%-                        gen_fragment callbacks                       -
%-=====================================================================-

%% @hidden

init ([ InitFragments, InitCopies, CopyType, Prefix ]) ->
  BlockMapTable = append_atom (Prefix, "_block_map"),
  BlockStoreTable = append_atom (Prefix, "_block_store"),
  DirectoryEntryTable = append_atom (Prefix, "_directory_entry"),
  InodeTable = append_atom (Prefix, "_inode"),
  MetaTable = append_atom (Prefix, "_meta"),
  SymlinkTable = append_atom (Prefix, "_symlink"),
  XattrTable = append_atom (Prefix, "_xattr"),

  gen_fragment:ensure_table
    (BlockMapTable,
     [ { attributes, record_info (fields, block_map) },
       { type, bag },
       { record_name, block_map },
       { frag_properties, [
         { hash_module, ?MODULE },
         { n_fragments, InitFragments },
         { node_pool, mnesia:system_info (running_db_nodes) },
         { CopyType, InitCopies }
       ] } ]),

  gen_fragment:ensure_table
    (BlockStoreTable,
     [ { attributes, record_info (fields, block_store) },
       { type, set },
       { record_name, block_store },
       { frag_properties, [
         { hash_module, ?MODULE },
         { n_fragments, InitFragments },
         { node_pool, mnesia:system_info (running_db_nodes) },
         { CopyType, InitCopies }
       ] } ]),

  gen_fragment:ensure_table
    (DirectoryEntryTable,
     [ { attributes, record_info (fields, directory_entry) },
       { type, ordered_set },
       { record_name, directory_entry },
       { frag_properties, [
         { hash_module, ?MODULE },
         { n_fragments, InitFragments },
         { node_pool, mnesia:system_info (running_db_nodes) },
         { ordered_set_type (CopyType), InitCopies }
       ] } ]),

  gen_fragment:ensure_table
    (InodeTable,
     [ { attributes, record_info (fields, inode) },
       { type, set },
       { record_name, inode },
       { frag_properties, [
         { hash_module, ?MODULE },
         { n_fragments, InitFragments },
         { node_pool, mnesia:system_info (running_db_nodes) },
         { CopyType, InitCopies }
       ] } ]),

  gen_fragment:ensure_table
    (MetaTable,
     [ { attributes, record_info (fields, meta) },
       { type, set },
       { record_name, meta },
       { frag_properties, [
         { hash_module, ?MODULE },
         { n_fragments, InitFragments },
         { node_pool, mnesia:system_info (running_db_nodes) },
         { CopyType, InitCopies }
       ] } ]),

  gen_fragment:ensure_table
    (SymlinkTable,
     [ { attributes, record_info (fields, symlink) },
       { type, set },
       { record_name, symlink },
       { frag_properties, [
         { hash_module, ?MODULE },
         { n_fragments, InitFragments },
         { node_pool, mnesia:system_info (running_db_nodes) },
         { CopyType, InitCopies }
       ] } ]),

  gen_fragment:ensure_table
    (XattrTable,
     [ { attributes, record_info (fields, xattr) },
       { type, ordered_set },
       { record_name, xattr },
       { frag_properties, [
         { hash_module, ?MODULE },
         { n_fragments, InitFragments },
         { node_pool, mnesia:system_info (running_db_nodes) },
         { ordered_set_type (CopyType), InitCopies }
       ] } ]),

  ok = make_root_directory (DirectoryEntryTable, InodeTable),

  { ok, 
    [ BlockMapTable,
      BlockStoreTable,
      DirectoryEntryTable,
      InodeTable,
      MetaTable,
      SymlinkTable,
      XattrTable ],
    #state{} }.

%% @hidden

handle_call (_Request, _From, State) -> { noreply, State }.

%% @hidden

handle_cast (_Request, State) -> { noreply, State }.

%% @hidden

handle_info (_Msg, State) -> { noreply, State }.

%% @hidden

terminate (_Reason, _State) -> ok.

%% @hidden

code_change (_OldVsn, State, _Extra) -> { ok, State }.

%-=====================================================================-
%-                      mnesia_frag_hash callbacks                     -
%-=====================================================================-

-record (frag_state, { tab, tab_type, num_fragments }).

%% @hidden

init_state (Tab, _State) ->
  TabType = lists:last (string:tokens (atom_to_list (Tab), "_")),

  #frag_state{ tab = Tab, tab_type = TabType, num_fragments = 1 }.

%% @hidden

add_frag (State = #frag_state{ num_fragments = NumFragments }) ->
  { State#frag_state{ num_fragments = NumFragments + 1 },
    lists:seq (1, NumFragments),
    [] }.

%% @hidden

del_frag (State = #frag_state{ num_fragments = NumFragments }) ->
  { State#frag_state{ num_fragments = NumFragments - 1 },
    lists:seq (1, NumFragments),
    [] }.

%% @hidden

key_to_frag_number (State = #frag_state{ tab_type = xattr }, { Inode, _ }) ->
  1 + erlang:phash2 (Inode, State#frag_state.num_fragments);
key_to_frag_number (State, Key) ->
  1 + erlang:phash2 (Key, State#frag_state.num_fragments).

%% @hidden

match_spec_to_frag_numbers (State = #frag_state{ tab_type = xattr },
                            MatchSpec) ->
  case MatchSpec of
    [ { #xattr{ id = Id = { Inode, _ } }, _, _ } ] ->
      case mnesia:has_var (Inode) of
        false ->
          [ key_to_frag_number (State, Id) ];
        true ->
          lists:seq (1, State#frag_state.num_fragments)
      end;
    _ ->
     lists:seq (1, State#frag_state.num_fragments)
  end;
match_spec_to_frag_numbers (State, MatchSpec) ->
  case MatchSpec of
    [ { HeadPat, _, _ } ] when is_tuple (HeadPat), size (HeadPat) > 2 ->
      KeyPat = element (2, HeadPat),
      case mnesia:has_var (KeyPat) of
        false ->
          [ key_to_frag_number (State, KeyPat) ];
        true ->
          lists:seq (1, State#frag_state.num_fragments)
      end;
    _ ->
     lists:seq (1, State#frag_state.num_fragments)
  end.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-

append_atom (X, Y) when is_atom (X), is_list (Y) ->
  list_to_atom (atom_to_list (X) ++ Y).

make_root_directory (DirectoryEntryTable, InodeTable) ->
  { Mega, Sec, _ } = erlang:now (),
  UnixNow = 1000000 * Mega + Sec,

  mnesia:activity 
    (sync_transaction,
     fun () ->
       case mnesia:read (InodeTable, 1, write) of
         [] ->
           ok = mnesia:write (InodeTable, 
                              #inode{ id = 1,
                                      stat = #stat{ st_ino = 1,
                                                    st_mode = ?S_IFDIR bor
                                                              ?S_IRUSR bor
                                                              ?S_IWUSR bor
                                                              ?S_IXUSR bor
                                                              ?S_IRGRP bor
                                                              ?S_IXGRP bor
                                                              ?S_IROTH bor
                                                              ?S_IXOTH,
                                                    st_nlink = 1,
                                                    st_atime = UnixNow,
                                                    st_ctime = UnixNow,
                                                    st_mtime = UnixNow } },
                              write),
           ok = mnesia:write (DirectoryEntryTable,
                              #directory_entry{ id = { 1, "." }, inode = 1 },
                              write),

           ok = mnesia:write (DirectoryEntryTable,
                              #directory_entry{ id = { 1, ".." }, inode = 1 },
                              write);
         _ ->
          ok
       end
      end,
      [],
      mnesia_frag).

ordered_set_type (n_disc_only_copies) -> n_disc_copies;
ordered_set_type (X) -> X.
