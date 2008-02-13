-module (walkenfssup).
-behaviour (supervisor).

-export ([ start_link/16, init/1 ]).

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
            ReuseNode,
            StartTimeout,
            StopTimeout,
            InitFragments,
            InitCopies,
            CopyType) ->
  supervisor:start_link (?MODULE, [ LinkedIn,
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
                                    CopyType ]).

%-=====================================================================-
%-                         supervisor callbacks                        -
%-=====================================================================-

%% @hidden

init ([ LinkedIn,
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
        CopyType ]) ->
  { ok,
    { { one_for_one, 3, 10 },
      [ { walkenfsfragmentsrv,
          { walkenfsfragmentsrv, start_link, [ ReuseNode,
                                               StartTimeout,
                                               InitFragments,
                                               InitCopies,
                                               CopyType,
                                               Prefix ] },
          permanent,
          StopTimeout,
          worker,
          [ walkenfsfragmentsrv ]
        },
        { walkenfssrv,
          { walkenfssrv, start_link, [ LinkedIn,
                                       MountOpts,
                                       MountPoint,
                                       Prefix,
                                       ReadDataContext,
                                       WriteDataContext,
                                       ReadMetaContext,
                                       WriteMetaContext,
                                       AttrTimeoutMs,
                                       EntryTimeoutMs,
                                       [] ] },
          permanent,
          StopTimeout,
          worker,
          [ walkenfssrv ]
        }
      ]
    }
  }.
