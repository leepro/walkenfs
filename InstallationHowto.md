# Prerequisites #

  1. gnu make
  1. working erlang installation.
    * debian: aptitude install erlang-dev
    * os/x (with [fink](http://www.finkproject.org/)): apt-get install erlang-otp
    * freebsd: pkg\_add -r erlang
    * others: ???
  1. [fuserl](http://code.google.com/p/fuserl)
  1. [fragmentron](http://code.google.com/p/fragmentron)
  1. [schemafinder](http://code.google.com/p/schemafinder) not technically required but highly recommended, since otherwise the examples below will not work, and you have to really know mnesia to get anywhere.
  1. [eunit](http://support.process-one.net/doc/display/CONTRIBS/EUnit) from process one; optional but 'make check' will not do anything without it.

# Installing #

Do NOT attempt to build from a checkout from the source code repository: that is for wizards only. Instead, grab one of the released source tarballs from the [downloads](http://code.google.com/p/walkenfs/downloads/list) tab.

The project uses automake[[2](#2.md)].  The default prefix is /usr; if you don't like that, use the `--prefix` option to configure.

Running `make check` is strongly suggested.

# Hello World #

After installing walkenfs, at your shell type
```
% mkdir /tmp/walken
% mkdir /tmp/walken-data
% sudo mount -t fuse 'walkenfs#-mnesia::dir::"/tmp/walken-data"::-s::combonodefinder::-s::schemafinder' /tmp/walken
```

The mount returns before the mount is completed, but within a few seconds you
should hopefully see
```
% sudo df /tmp/walken
Filesystem           1K-blocks      Used Available Use% Mounted on
/dev/fuse             14680064        38  14680027   1% /tmp/walken
```
You (root) can now do normal filesystem operations.  Call umount
to discard when finished.

Note there is no mkfs.walken.  It happens automatically when you start
walkenfs for the first time.[[1](#1.md)]

## Next Steps ##

If you want to use walkenfs "for real" then you probably want synchronous
mount and umount, plus you want to set all the fuse mount options that
make walkenfs act normally with respect to file access permissions.
You might also want a bigger filesystem.

There is an
[example init script](http://code.google.com/p/walkenfs/source/browse/trunk/src/walkenfs.example) which starts and stops walkenfs.

If you're using walkenfs on EC2, do not create the walkenfs filesystem
on your image stored in S3; this doesn't work because the node name will be different when the image is booted.  Instead let walkenfs autocreate it
when your image boots up.  This implies walkenfs needs to be started
after the network is up so it can find it's Erlang peers via schemafinder.

# Tested Platforms #

  * Ubuntu 32 and 64 bit Linux (feisty and gusty).
  * OS/X 10.4 i386: known **not** to work, I'm trying to figure it out.

# Footnotes #

## 1 ##

Why is there no mkfs.walken?  Because it boils down to specifying
a distributed mnesia config.  Those who know how to do that (wizards!)
would use the erlang shell to do it.  For the rest of us schemafinder
is the way to go, especially on EC2 where specifying the configuration
manually is unrealistic given the dynamic nature of the cluster.

## 2 ##

More precisely, downloadable source tarballs use automake to build. A source code checkout of the repository uses [fwtemplates](http://code.google.com/p/framewerk) to build, and if you don't know what that is, you probably want to stick with the tarballs.