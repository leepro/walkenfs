walkenfs is a distributed filesystem written in Erlang (via [fuse](http://fuse.sourceforge.net/)), which leverages Erlang's built-in distributed multi-master database ([Mnesia](http://www.erlang.org/doc/apps/mnesia/index.html)).  The design goal is ease of use given a dynamic set of node resources (such as [EC2](http://www.amazon.com/gp/browse.html?node=201590011)).

It requires [fuserl](http://code.google.com/p/fuserl), [fragmentron](http://code.google.com/p/fragmentron), and [schemafinder](http://code.google.com/p/schemafinder) so if you're interested start there.

Right now only Linux passes 'make check'.

Another [Dukes of Erl](http://dukesoferl.blogspot.com/) release.